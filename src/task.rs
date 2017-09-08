use Notifier;

use futures::{future, Future, Async};
use futures::executor::{self, Spawn};

use std::{fmt, mem, ptr};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{self, AtomicUsize, AtomicPtr};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release, Relaxed};

pub(crate) struct Task {
    ptr: *mut Inner,
}

#[derive(Debug)]
pub(crate) struct Queue {
    head: AtomicPtr<Inner>,
    tail: Cell<*mut Inner>,
    stub: Box<Inner>,
}

#[derive(Debug)]
pub(crate) enum Poll {
    Empty,
    Inconsistent,
    Data(Task),
}

struct Inner {
    // Next pointer in the queue that submits tasks to a worker.
    next: AtomicPtr<Inner>,

    // Task state
    state: AtomicUsize,

    // Number of outstanding references to the task
    ref_count: AtomicUsize,

    // Store the future at the head of the struct
    future: Spawn<BoxFuture>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum State {
    /// Task is currently idle
    Idle,
    /// Task is currently running
    Running,
    /// Task is currently running, but has been notified that it must run again.
    Notified,
    /// Task has been scheduled
    Scheduled,
    /// Task is complete
    Complete,
}

type BoxFuture = Box<Future<Item = (), Error = ()> + Send + 'static>;



// ===== impl Task =====

impl Task {
    /// Create a new task handle
    pub fn new<T: Future<Item = (), Error = ()> + Send + 'static>(f: T) -> Task {
        let inner = Box::new(Inner {
            next: AtomicPtr::new(ptr::null_mut()),
            state: AtomicUsize::new(State::new().into()),
            ref_count: AtomicUsize::new(1),
            future: executor::spawn(Box::new(f)),
        });

        Task { ptr: Box::into_raw(inner) }
    }

    /// Transmute a u64 to a Task
    pub unsafe fn from_notify_id(unpark_id: usize) -> Task {
        mem::transmute(unpark_id)
    }

    /// Transmute a u64 to a task ref
    pub unsafe fn from_notify_id_ref<'a>(unpark_id: &'a usize) -> &'a Task {
        mem::transmute(unpark_id)
    }

    /// Execute the task returning `true` if the task needs to be scheduled again.
    pub fn run(&self, unpark: &Arc<Notifier>) -> bool {
        use self::State::*;

        // Transition task to running state. At this point, the task must be
        // scheduled.
        let actual: State = self.inner().state.compare_and_swap(
            Scheduled.into(), Running.into(), AcqRel).into();

        match actual {
            Scheduled => {},
            _ => panic!("unexpected task state; {:?}", actual),
        }

        trace!("Task::run; state={:?}", State::from(self.inner().state.load(Relaxed)));

        let res = self.inner_mut().future
            .poll_future_notify(unpark, self.ptr as usize);

        match res {
            Ok(Async::Ready(_)) | Err(_) => {
                trace!("    -> task complete");
                // Transition to the completed state
                self.inner().state.store(State::Complete.into(), Release);
                false
            }
            _ => {
                trace!("    -> not ready");

                // Attempt to transition from Running -> Idle, if successful,
                // then the task does not need to be scheduled again. If the CAS
                // fails, then the task has been unparked concurrent to running,
                // in which case it transitions immediately back to scheduled
                // and we return `true`.
                let prev: State = self.inner().state.compare_and_swap(
                    Running.into(), Idle.into(), AcqRel).into();

                match prev {
                    Running => false,
                    Notified => {
                        self.inner().state.store(Scheduled.into(), Release);
                        true
                    }
                    _ => unreachable!(),
                }
            }
        }
    }

    /// Transition the task state to scheduled.
    ///
    /// Returns `true` if the caller is permitted to schedule the task.
    pub fn schedule(&self) -> bool {
        use self::State::*;

        loop {
            let actual = self.inner().state.compare_and_swap(
                Idle.into(),
                Scheduled.into(),
                Relaxed).into();

            match actual {
                Idle => return true,
                Running => {
                    let actual = self.inner().state.compare_and_swap(
                        Running.into(), Notified.into(), Relaxed).into();

                    match actual {
                        Idle => continue,
                        _ => return false,
                    }
                }
                Complete | Notified | Scheduled => return false,
            }
        }
    }

    #[inline]
    fn inner(&self) -> &Inner {
        unsafe { &*self.ptr }
    }

    #[inline]
    fn inner_mut(&self) -> &mut Inner {
        unsafe { &mut *self.ptr }
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Task")
            .field("inner", self.inner())
            .finish()
    }
}

impl Clone for Task {
    fn clone(&self) -> Task {
        use std::isize;

        const MAX_REFCOUNT: usize = (isize::MAX) as usize;
        // Using a relaxed ordering is alright here, as knowledge of the
        // original reference prevents other threads from erroneously deleting
        // the object.
        //
        // As explained in the [Boost documentation][1], Increasing the
        // reference counter can always be done with memory_order_relaxed: New
        // references to an object can only be formed from an existing
        // reference, and passing an existing reference from one thread to
        // another must already provide any required synchronization.
        //
        // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        let old_size = self.inner().ref_count.fetch_add(1, Relaxed);

        // However we need to guard against massive refcounts in case someone
        // is `mem::forget`ing Arcs. If we don't do this the count can overflow
        // and users will use-after free. We racily saturate to `isize::MAX` on
        // the assumption that there aren't ~2 billion threads incrementing
        // the reference count at once. This branch will never be taken in
        // any realistic program.
        //
        // We abort because such a program is incredibly degenerate, and we
        // don't care to support it.
        if old_size > MAX_REFCOUNT {
            // TODO: abort
            panic!();
        }

        Task { ptr: self.ptr }
    }
}

impl Drop for Task {
    fn drop(&mut self) {
        // Because `fetch_sub` is already atomic, we do not need to synchronize
        // with other threads unless we are going to delete the object. This
        // same logic applies to the below `fetch_sub` to the `weak` count.
        if self.inner().ref_count.fetch_sub(1, Release) != 1 {
            return;
        }

        // This fence is needed to prevent reordering of use of the data and
        // deletion of the data.  Because it is marked `Release`, the decreasing
        // of the reference count synchronizes with this `Acquire` fence. This
        // means that use of the data happens before decreasing the reference
        // count, which happens before this fence, which happens before the
        // deletion of the data.
        //
        // As explained in the [Boost documentation][1],
        //
        // > It is important to enforce any possible access to the object in one
        // > thread (through an existing reference) to *happen before* deleting
        // > the object in a different thread. This is achieved by a "release"
        // > operation after dropping a reference (any access to the object
        // > through this reference must obviously happened before), and an
        // > "acquire" operation before deleting the object.
        //
        // [1]: (www.boost.org/doc/libs/1_55_0/doc/html/atomic/usage_examples.html)
        atomic::fence(Acquire);

        unsafe {
            let _ = Box::from_raw(self.ptr);
        }
    }
}

unsafe impl Send for Task {}

// ===== impl Inner =====

impl Inner {
    fn stub() -> Inner {
        Inner {
            next: AtomicPtr::new(ptr::null_mut()),
            state: AtomicUsize::new(State::stub().into()),
            ref_count: AtomicUsize::new(0),
            future: executor::spawn(Box::new(future::empty())),
        }
    }
}

impl fmt::Debug for Inner {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Inner")
            .field("next", &self.next)
            .field("state", &self.state)
            .field("ref_count", &self.ref_count)
            .field("future", &"Spawn<BoxFuture>")
            .finish()
    }
}

// ===== impl Queue =====

impl Queue {
    pub fn new() -> Queue {
        let stub = Box::new(Inner::stub());
        let ptr = &*stub as *const _ as *mut _;

        Queue {
            head: AtomicPtr::new(ptr),
            tail: Cell::new(ptr),
            stub: stub,
        }
    }

    pub fn push(&self, handle: Task) {
        unsafe {
            self.push2(handle.ptr);

            // Forgetting the handle is necessary to avoid the ref dec
            mem::forget(handle);
        }
    }

    unsafe fn push2(&self, handle: *mut Inner) {
        // Set the next pointer. This does not require an atomic operation as
        // this node is not accessible. The write will be flushed with the next
        // operation
        (*handle).next = AtomicPtr::new(ptr::null_mut());

        // Update the head to point to the new node. We need to see the previous
        // node in order to update the next pointer as well as release `handle`
        // to any other threads calling `push`.
        let prev = self.head.swap(handle, AcqRel);

        // Release `handle` to the consume end.
        (*prev).next.store(handle, Release);
    }

    pub unsafe fn poll(&self) -> Poll {
        let mut tail = self.tail.get();
        let mut next = (*tail).next.load(Acquire);
        let stub = &*self.stub as *const _ as *mut _;

        if tail == stub {
            if next.is_null() {
                return Poll::Empty;
            }

            self.tail.set(next);
            tail = next;
            next = (*next).next.load(Acquire);
        }

        if !next.is_null() {
            self.tail.set(next);

            // No ref_count inc is necessary here as this poll is paired
            // with a `push` which "forgets" the handle.
            return Poll::Data(Task {
                ptr: tail,
            });
        }

        if self.head.load(Acquire) != tail {
            return Poll::Inconsistent;
        }

        self.push2(stub);

        next = (*tail).next.load(Acquire);

        if !next.is_null() {
            self.tail.set(next);
            return Poll::Data(Task {
                ptr: tail,
            });
        }

        Poll::Inconsistent
    }
}

// ===== impl State =====

impl State {
    /// Returns the initial task state.
    ///
    /// Tasks start in the scheduled state as they are immediately scheduled on
    /// creation.
    fn new() -> State {
        State::Scheduled
    }

    fn stub() -> State {
        State::Idle
    }
}

impl From<usize> for State {
    fn from(src: usize) -> Self {
        use self::State::*;

        match src {
            0 => Idle,
            1 => Running,
            2 => Notified,
            3 => Scheduled,
            4 => Complete,
            _ => unreachable!(),
        }
    }
}

impl From<State> for usize {
    fn from(src: State) -> Self {
        use self::State::*;

        match src {
            Idle => 0,
            Running => 1,
            Notified => 2,
            Scheduled => 3,
            Complete => 4,
        }
    }
}
