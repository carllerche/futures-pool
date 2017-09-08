//! A work-stealing based thread pool for executing futures.

#![deny(warnings, missing_docs, missing_debug_implementations)]

extern crate futures;
extern crate num_cpus;
extern crate rand;

#[macro_use]
extern crate log;

mod deque;
mod task;

use deque::Deque;
use task::Task;

use futures::Future;
use futures::executor::Notify;
use futures::future::{Executor, ExecuteError};

use rand::{Rng, SeedableRng, XorShiftRng};

use std::{fmt, mem, thread, usize};
use std::cell::{Cell, UnsafeCell};
use std::sync::{Arc, Weak, Mutex, Condvar};
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering::{AcqRel, Acquire, Release, Relaxed};
use std::time::Duration;

/// Thread pool
#[derive(Debug)]
pub struct Pool {
    inner: Arc<Inner>,
}

/// Sends work to the pool
#[derive(Debug)]
pub struct Sender {
    inner: Arc<Inner>,
}

/// Pool configuration
#[derive(Debug)]
pub struct Builder {
    /// Thread pool specific configuration values
    config: Config,

    /// Number of workers to spawn
    pool_size: usize,
}

/// Thread pool specific configuration values
#[derive(Debug, Clone)]
struct Config {
    keep_alive: Option<Duration>,
    // Used to configure a worker thread
    name_prefix: Option<String>,
    stack_size: Option<usize>,
    after_start: Option<Callback>,
    before_stop: Option<Callback>,
}

#[derive(Debug)]
struct Inner {
    // Pool state
    //
    // Tracks:
    // * Total worker count
    // * Active worker count
    // * Pool lifecycle
    state: AtomicUsize,

    // Used to generate a thread local RNG seed
    next_thread_id: AtomicUsize,

    // Storage for workers
    //
    // This will *usually* be a small number
    workers: Box<[WorkerEntry]>,

    // Configuration
    config: Config,
}

#[derive(Clone)]
struct Callback {
    f: Arc<Fn() + Send + Sync>,
}

/// Implements the future `Notify` API.
///
/// This is how external events are able to signal the task, informing it to try
/// to poll the future again.
#[derive(Debug)]
struct Notifier {
    inner: Weak<Inner>,
}

/// Pool state packed into an AtomicUsize
///
/// Tracks:
/// - Lifecycle        2 bits
/// - Parked head     15 bits
/// - Sequence        remaining
#[derive(Debug, Eq, PartialEq, Clone, Copy)]
struct State(usize);

/// Extracts the lifecycle component from the scheduler state
const LIFECYCLE_MASK: usize = 0b11;

/// The scheduler is running
const RUNNING: usize = 0b00;

/*
/// The scheduler is shutting down
const SHUTDOWN: usize = 0b01;

/// The scheduler has stopped, this is more aggressive than shutdown.
const STOP: usize = 0b10;

/// The scheduler has terminated
const TERMINATED: usize = 0b11;
*/

/// Extracts the head of the worker stack from the scheduler state
const STACK_MASK: usize = ((1 << 15) - 1) << STACK_SHIFT;

/// Max number of workers that can be part of a pool. This is the most that can
/// fit in the scheduler state. Note, that this is the max number of **active**
/// threads. There can be more standby threads.
const MAX_WORKERS: usize = 1 << 14;

/// How many bits the stack head index is offset
const STACK_SHIFT: usize = 2;

/// How many bits the treiber ABA guard is offset by
const ABA_GUARD_SHIFT: usize = 15 + 2;

#[cfg(target_pointer_width = "64")]
const ABA_GUARD_MASK: usize = (1 << (64 - ABA_GUARD_SHIFT)) - 1;

#[cfg(target_pointer_width = "32")]
const ABA_GUARD_MASK: usize = (1 << (32 - ABA_GUARD_SHIFT)) - 1;

// Some constants used to work with State
// const A: usize: 0;

// TODO: This should be split up between what is accessed by each thread and
// what is concurrent. The bits accessed by each thread should be sized to
// exactly one cache line.
#[derive(Debug)]
struct WorkerEntry {
    // Worker state
    state: AtomicUsize,

    // Next entry in the parked Trieber stack
    next_sleeper: UnsafeCell<usize>,

    // Work-stealing deque
    deque: Deque<Task>,

    // Park mutex
    park_mutex: Mutex<()>,

    // Park condvar
    park_condvar: Condvar,

    // MPSC queue of jobs submitted to the worker from an external source.
    inbound: task::Queue,
}

/// Tracks worker state
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
struct WorkerState(usize);

/// Set when the worker is pushed onto the scheduler's stack of sleeping
/// threads.
const PUSHED_MASK: usize = 0b001;

/// Manages the worker lifecycle part of the state
const WORKER_LIFECYCLE_MASK: usize = 0b1110;
const WORKER_LIFECYCLE_SHIFT: usize = 1;

/// The worker does not currently have an associated thread.
const WORKER_SHUTDOWN: usize = 0;

/// The worker is currently processing its task.
const WORKER_RUNNING: usize = 1;

/// The worker is currently asleep in the condvar
const WORKER_SLEEPING: usize = 2;

/// The worker has been notified it should process more work.
const WORKER_NOTIFIED: usize = 3;

/// A stronger form of notification. In this case, the worker is expected to
/// wakeup and try to acquire more work... if it enters this state while already
/// busy with other work, it is expected to signal another worker.
const WORKER_SIGNALED: usize = 4;

struct Worker {
    // Shared scheduler data
    inner: Arc<Inner>,

    // WorkerEntry index
    idx: usize,
}

// Pointer to the current worker info
thread_local!(static CURRENT_WORKER: Cell<*const Worker> = Cell::new(0 as *const _));

// ===== impl Builder =====

impl Builder {
    /// Returns a builder with default values
    pub fn new() -> Builder {
        let num_cpus = num_cpus::get();

        Builder {
            pool_size: num_cpus,
            config: Config {
                keep_alive: None,
                name_prefix: None,
                stack_size: None,
                after_start: None,
                before_stop: None,
            },
        }
    }

    /// Set the scheduler's pool size
    pub fn pool_size(&mut self, val: usize) -> &mut Self {
        assert!(val >= 1, "at least one thread required");

        self.pool_size = val;
        self
    }

    /// Set the thread keep alive duration
    pub fn keep_alive(&mut self, val: Duration) -> &mut Self {
        self.config.keep_alive = Some(val);
        self
    }

    /// Set name prefix of threads spawned by the scheduler
    ///
    /// Thread name prefix is used for generating thread names. For example, if
    /// prefix is `my-pool-`, then threads in the pool will get names like
    /// `my-pool-1` etc.
    pub fn name_prefix<S: Into<String>>(&mut self, val: S) -> &mut Self {
        self.config.name_prefix = Some(val.into());
        self
    }

    /// Set the stack size of threads spawned by the scheduler
    pub fn stack_size(&mut self, val: usize) -> &mut Self {
        self.config.stack_size = Some(val);
        self
    }

    /// Execute function `f` right after each thread is started but before
    /// running any tasks on it
    ///
    /// This is initially intended for bookkeeping and monitoring uses
    pub fn after_start<F>(&mut self, f: F) -> &mut Self
        where F: Fn() + Send + Sync + 'static
    {
        self.config.after_start = Some(Callback::new(f));
        self
    }

    /// Execute function `f` before each worker thread stops
    ///
    /// This is initially intended for bookkeeping and monitoring uses
    pub fn before_stop<F>(&mut self, f: F) -> &mut Self
        where F: Fn() + Send + Sync + 'static
    {
        self.config.before_stop = Some(Callback::new(f));
        self
    }

    /// Build and return the configured thread pool
    pub fn build(&self) -> (Sender, Pool) {
        let mut workers = vec![];

        for _ in 0..self.pool_size {
            workers.push(WorkerEntry::new());
        }

        let inner = Arc::new(Inner {
            state: AtomicUsize::new(State::new().into()),
            next_thread_id: AtomicUsize::new(0),
            workers: workers.into_boxed_slice(),
            config: self.config.clone(),
        });

        // Now, we prime the sleeper stack
        for i in 0..self.pool_size {
            inner.push_sleeper(i)
        }

        let sender = Sender {
            inner: inner.clone(),
        };

        let scheduler = Pool {
            inner: inner,
        };

        (sender, scheduler)
    }
}

// ===== impl Pool =====

impl Pool {
    /// Create a new Pool with default configuration.
    pub fn new() -> (Sender, Pool) {
        Builder::new().build()
    }

    /// Start a core thread, causing it to idly wait for work.
    ///
    /// This overrides the default policy of starting core threads only when new
    /// tasks are executed. This function will return `false` if all core
    /// threads have already been started.
    pub fn prestart_core_thread(&self) -> bool {
        unimplemented!();
    }

    /// Start all core threads, causing them to idly wait for work.
    ///
    /// This overrides the default policy of starting core threads only when new
    /// tasks are executed.
    pub fn prestart_core_threads(&self) {
        while self.prestart_core_thread() {}
    }

    /// Returns true if the pool is currently running.
    pub fn is_running(&self) -> bool {
        let state: State = self.inner.state.load(Relaxed).into();
        state.is_running()
    }
}

// ===== impl Sender ======

impl Sender {
}

impl<T> Executor<T> for Sender
    where T: Future<Item = (), Error = ()> + Send + 'static,
{
    fn execute(&self, f: T) -> Result<(), ExecuteError<T>> {
        let task = Task::new(f);
        // TODO: handle the error
        let _ = self.inner.submit(task, &self.inner);
        Ok(())
    }
}

impl Clone for Sender {
    #[inline]
    fn clone(&self) -> Sender {
        Sender {
            inner: self.inner.clone(),
        }
    }
}

// ===== impl Inner =====

macro_rules! worker_loop {
    ($probe_var:ident = $init:expr; $len:expr; $body:expr) => {{
        let mut $probe_var = $init;
        let start = $probe_var;
        let len = $len;

        loop {
            if $probe_var < len {
                $body
                $probe_var += 1;
            } else {
                $probe_var = 0;
            }

            if $probe_var == start {
                break;
            }
        }
    }};
}

impl Inner {
    /// Submit a task to the scheduler.
    ///
    /// Called from either inside or outside of the scheduler. If currently on
    /// the scheduler, then a fast path is taken.
    fn submit(&self, task: Task, inner: &Arc<Inner>) -> Result<(), Task> {
        // TODO: Check if the scheduler is still running.

        Worker::with_current(|worker| {
            match worker {
                Some(worker) => {
                    trace!("    -> submit internal");
                    let idx = worker.idx;
                    worker.inner.workers[idx].submit_internal(task);
                    worker.inner.signal_work(inner);
                }
                None => {
                    self.submit_external(task, inner);
                }
            }
        });

        Ok(())
    }

    /// Submit a task to the scheduler from off worker
    ///
    /// Called from outside of the scheduler, this function is how new tasks
    /// enter the system.
    fn submit_external(&self, task: Task, inner: &Arc<Inner>) {
        // First try to get a handle to a sleeping worker. This ensures that
        // sleeping tasks get woken up
        if let Some((idx, state)) = self.pop_sleeper(WORKER_NOTIFIED) {
            self.submit_to_external(idx, task, state, inner);
            return;
        }

        // All workers are active, so pick a random worker and submit the
        // task to it.
        let len = self.workers.len();
        let idx = self.rand_usize() % len;

        let state: WorkerState = self.workers[idx].state.load(Acquire).into();
        self.submit_to_external(idx, task, state, inner);
    }

    fn submit_to_external(&self,
                          idx: usize,
                          task: Task,
                          state: WorkerState,
                          inner: &Arc<Inner>)
    {
        let entry = &self.workers[idx];

        if !entry.submit_external(task, state) {
            Worker::spawn(idx, inner);
        }
    }

    /// If there are any other workers currently relaxing, signal them that work
    /// is available so that they can try to find more work to process.
    fn signal_work(&self, inner: &Arc<Inner>) {
        if let Some((idx, mut state)) = self.pop_sleeper(WORKER_SIGNALED) {
            let entry = &self.workers[idx];

            // Transition the worker state to signaled
            loop {
                let mut next = state;

                // pop_sleeper should skip these
                debug_assert!(state.lifecycle() != WORKER_SIGNALED);
                next.set_lifecycle(WORKER_SIGNALED);

                let actual = entry.state.compare_and_swap(
                    state.into(), next.into(), AcqRel).into();

                if actual == state {
                    break;
                }

                state = actual;
            }

            // The state has been transitioned to signal, now we need to wake up
            // the worker if necessary.
            match state.lifecycle() {
                WORKER_SLEEPING => {
                    self.workers[idx].wakeup();
                }
                WORKER_SHUTDOWN => {
                    Worker::spawn(idx, inner);
                }
                _ => {}
            }
        }
    }

    fn push_sleeper(&self, idx: usize) {
        let mut state: State = self.state.load(Acquire).into();

        debug_assert!(WorkerState::from(self.workers[idx].state.load(Relaxed)).is_pushed());

        loop {
            let mut next = state;

            self.workers[idx].set_next_sleeper(state.sleeper_head());
            next.set_sleeper_head(idx);

            let actual = self.state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if state == actual {
                return;
            }

            state = actual;
        }
    }

    fn pop_sleeper(&self, max_lifecycle: usize) -> Option<(usize, WorkerState)> {
        let mut state: State = self.state.load(Acquire).into();

        loop {
            let head = state.sleeper_head();

            if head == MAX_WORKERS {
                return None;
            }

            let mut next = state;
            next.set_sleeper_head(self.workers[head].next_sleeper());

            let actual = self.state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                // The worker has been removed from the stack, so the pushed bit
                // can be unset. Release ordering is used to ensure that this
                // operation happens after actually popping the task.
                debug_assert_eq!(1, PUSHED_MASK);

                // Unset the PUSHED flag and get the current state.
                let state: WorkerState = self.workers[head].state
                    .fetch_sub(PUSHED_MASK, Release).into();

                if state.lifecycle() >= max_lifecycle {
                    // If the worker has already been notified, then it is
                    // warming up to do more work. In this case, try to pop
                    // another thread that might be in a relaxed state.
                    continue;
                }

                return Some((head, state));
            }

            state = actual;
        }
    }

    /// Generates a random number
    ///
    /// Uses a thread-local seeded XorShift.
    fn rand_usize(&self) -> usize {
        // Use a thread-local random number generator. If the thread does not
        // have one yet, then seed a new one
        thread_local!(static THREAD_RNG_KEY: UnsafeCell<Option<XorShiftRng>> = UnsafeCell::new(None));

        THREAD_RNG_KEY.with(|t| {
            #[cfg(target_pointer_width = "32")]
            fn new_rng(thread_id: usize) -> XorShiftRng {
                XorShiftRng::from_seed([
                    thread_id as u32,
                    0x00000000,
                    0xa8a7d469,
                    0x97830e05])
            }

            #[cfg(target_pointer_width = "64")]
            fn new_rng(thread_id: usize) -> XorShiftRng {
                XorShiftRng::from_seed([
                    thread_id as u32,
                    (thread_id >> 32) as u32,
                    0xa8a7d469,
                    0x97830e05])
            }

            let thread_id = self.next_thread_id.fetch_add(1, Relaxed);
            let rng = unsafe { &mut *t.get() };

            if rng.is_none() {
                *rng = Some(new_rng(thread_id));
            }

            rng.as_mut().unwrap().next_u32() as usize
        })
    }
}

impl Notify for Notifier {
    fn notify(&self, id: usize) {
        trace!("Notifier::notify; id=0x{:x}", id);

        let id = id as usize;
        let task = unsafe { Task::from_notify_id_ref(&id) };

        if !task.schedule() {
            trace!("    -> task already scheduled");
            // task is already scheduled, there is nothing more to do
            return;
        }

        // Bump the ref count
        let task = task.clone();

        if let Some(inner) = self.inner.upgrade() {
            let _ = inner.submit(task, &inner);
        }
    }

    fn clone_id(&self, id: usize) -> usize {
        unsafe {
            let handle = Task::from_notify_id_ref(&id);
            mem::forget(handle.clone());
        }

        id
    }

    fn drop_id(&self, id: usize) {
        unsafe {
            let _ = Task::from_notify_id(id);
        }
    }
}

unsafe impl Send for Inner {}
unsafe impl Sync for Inner {}

// ===== impl Worker =====

impl Worker {
    fn spawn(idx: usize, inner: &Arc<Inner>) {
        trace!("spawning new worker thread");

        let worker = Worker {
            inner: inner.clone(),
            idx: idx,
        };

        let mut th = thread::Builder::new();

        if let Some(ref prefix) = inner.config.name_prefix {
            th = th.name(format!("{}{}", prefix, idx));
        }

        if let Some(stack) = inner.config.stack_size {
            th = th.stack_size(stack);
        }

        th.spawn(move || {
            if let Some(ref f) = worker.inner.config.after_start {
                f.call();
            }

            worker.run();

            if let Some(ref f) = worker.inner.config.before_stop {
                f.call();
            }
        }).unwrap();
    }

    fn with_current<F: FnOnce(Option<&Worker>) -> R, R>(f: F) -> R {
        CURRENT_WORKER.with(move |c| {
            let ptr = c.get();

            if ptr.is_null() {
                f(None)
            } else {
                f(Some(unsafe { &*ptr }))
            }
        })
    }

    fn run(&self) {
        CURRENT_WORKER.with(move |c| {
            c.set(self as *const _);
            self.run2();
        });
    }

    fn run2(&self) {
        // Get the notifier.
        //
        // TODO: Should this be shared across workers? Probably doesn't matter
        // much?
        let notify = Arc::new(Notifier {
            inner: Arc::downgrade(&self.inner),
        });

        let mut first = true;
        let mut spin_cnt = 0;

        while self.check_run_state(first) {
            first = false;

            // Poll inbound until empty, transfering all tasks to the internal
            // queue.
            let consistent = self.drain_inbound();

            // Run the next available task
            if self.try_run_task(&notify) {
                spin_cnt = 0;
                // As long as there is work, keep looping.
                continue;
            }

            // No work in this worker's queue, it is time to try stealing.
            if self.try_steal_task(&notify) {
                spin_cnt = 0;
                continue;
            }

            if !consistent {
                spin_cnt = 0;
                continue;
            }

            // Starting to get sleeeeepy
            if spin_cnt < 32 {
                spin_cnt += 1;

                // Don't do anything further
            } else if spin_cnt < 256 {
                spin_cnt += 1;

                // Yield the thread
                thread::yield_now();
            } else {
                self.sleep();
            }

            // If there still isn't any work to do, shutdown the worker?
        }
    }

    /// Checks the worker's current state, updating it as needed.
    ///
    /// Returns `true` if the worker should run.
    #[inline]
    fn check_run_state(&self, first: bool) -> bool {
        let mut state: WorkerState = self.entry().state.load(Acquire).into();

        loop {
            // TODO: ensure scheduler still running
            //
            let mut next = state;

            match state.lifecycle() {
                WORKER_RUNNING => break,
                WORKER_NOTIFIED | WORKER_SIGNALED => {
                    // transition back to running
                    next.set_lifecycle(WORKER_RUNNING);
                }
                lifecycle => panic!("unexpected worker state; lifecycle={}", lifecycle),
            }

            let actual = self.entry().state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                break;
            }

            state = actual;
        }

        // If this is the first iteration of the worker loop, then the state can
        // be signaled.
        if !first && state.is_signaled() {
            // This worker is not ready to be signaled, so delegate the signal
            // to another worker.
            self.inner.signal_work(&self.inner);
        }

        true
    }

    /// Runs the next task on this worker's queue.
    ///
    /// Returns `true` if work was found.
    #[inline]
    fn try_run_task(&self, notify: &Arc<Notifier>) -> bool {
        use deque::Poll::*;

        // Poll the internal queue for a task to run
        match self.entry().deque.poll() {
            Data(task) => {
                self.run_task(task, notify);
                true
            }
            Empty => false,
            Inconsistent => true,
        }
    }

    /// Tries to steal a task from another worker.
    ///
    /// Returns `true` if work was found
    #[inline]
    fn try_steal_task(&self, notify: &Arc<Notifier>) -> bool {
        use deque::Poll::*;

        let len = self.inner.workers.len();

        let mut found_work = false;

        worker_loop!(idx = len; len; {
            match self.inner.workers[idx].deque.poll() {
                Data(task) => {
                    self.run_task(task, notify);

                    // Signal other workers that work is available
                    self.inner.signal_work(&self.inner);

                    return true;
                }
                Empty => {}
                Inconsistent => found_work = true,
            }
        });

        found_work
    }

    fn run_task(&self, task: Task, notify: &Arc<Notifier>) {
        if task.run(notify) {
            self.entry().push_internal(task);
        }
    }

    /// Drains all tasks on the extern queue and pushes them onto the internal
    /// queue.
    ///
    /// Returns `true` if the operation was able to complete in a consistent
    /// state.
    #[inline]
    fn drain_inbound(&self) -> bool {
        use task::Poll::*;

        let mut found_work = false;

        loop {
            let task = unsafe { self.entry().inbound.poll() };

            match task {
                Empty => {
                    if found_work {
                        self.inner.signal_work(&self.inner);
                    }

                    return true;
                }
                Inconsistent => {
                    if found_work {
                        self.inner.signal_work(&self.inner);
                    }

                    return false;
                }
                Data(task) => {
                    found_work = true;
                    self.entry().push_internal(task);
                }
            }
        }
    }
    /// Put the worker to sleep
    ///
    /// Returns `true` if woken up due to new work arriving.
    #[inline]
    fn sleep(&self) -> bool {
        trace!("Worker::sleep");

        let mut state: WorkerState = self.entry().state.load(Acquire).into();

        // The first part of the sleep process is to transition the worker state
        // to "pushed". Now, it may be that the worker is already pushed on the
        // sleeper stack, in which case, we don't push again. However, part of
        // this process is also to do some final state checks to avoid entering
        // the mutex if at all possible.

        loop {
            let mut next = state;

            match state.lifecycle() {
                WORKER_RUNNING => {
                    // Try setting the pushed state
                    next.set_pushed();
                }
                WORKER_NOTIFIED | WORKER_SIGNALED => {
                    // No need to sleep, transition back to running and move on.
                    next.set_lifecycle(WORKER_RUNNING);
                }
                actual => panic!("unexpected worker state; {}", actual),
            }

            let actual = self.entry().state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                if state.is_notified() {
                    // The previous state was notified, so we don't need to
                    // sleep.
                    return true;
                }

                if !state.is_pushed() {
                    debug_assert!(next.is_pushed());

                    // We obtained permission to push the worker into the
                    // sleeper queue.
                    self.inner.push_sleeper(self.idx);
                }

                break;
            }

            state = actual;
        }

        // Acquire the sleep mutex, the state is transitioned to sleeping within
        // the mutex in order to avoid losing wakeup notifications.
        let mut lock = self.entry().park_mutex.lock().unwrap();

        // Transition the state to sleeping, a CAS is still needed as other
        // state transitions could happen unrelated to the sleep / wakeup
        // process. We also have to redo the lifecycle check done above as
        // the state could have been transitioned before entering the mutex.
        loop {
            let mut next = state;

            match state.lifecycle() {
                WORKER_RUNNING => {}
                WORKER_NOTIFIED | WORKER_SIGNALED => {
                    // Release the lock, sleep will not happen this call.
                    drop(lock);

                    // Transition back to running
                    loop {
                        let mut next = state;
                        next.set_lifecycle(WORKER_RUNNING);

                        let actual = self.entry().state.compare_and_swap(
                            state.into(), next.into(), AcqRel).into();

                        if actual == state {
                            return true;
                        }

                        state = actual;
                    }
                }
                _ => unreachable!(),
            }

            next.set_lifecycle(WORKER_SLEEPING);

            let actual = self.entry().state.compare_and_swap(
                state.into(), next.into(), AcqRel).into();

            if actual == state {
                break;
            }

            state = actual;
        }

        trace!("    -> starting to sleep");

        // The state has been transitioned to sleeping, we can now wait on the
        // condvar. This is done in a loop as condvars can wakeup spuriously.
        loop {
            lock = self.entry().park_condvar.wait(lock).unwrap();

            trace!("    -> wakeup");

            // Reload the state
            state = self.entry().state.load(Acquire).into();

            match state.lifecycle() {
                WORKER_SLEEPING => {}
                WORKER_NOTIFIED | WORKER_SIGNALED => {
                    // Release the lock, done sleeping
                    drop(lock);

                    // Transition back to running
                    loop {
                        let mut next = state;
                        next.set_lifecycle(WORKER_RUNNING);

                        let actual = self.entry().state.compare_and_swap(
                            state.into(), next.into(), AcqRel).into();

                        if actual == state {
                            return true;
                        }

                        state = actual;
                    }
                }
                _ => unreachable!(),
            }

            // The worker hasn't been notified, go back to sleep
        }
    }

    fn entry(&self) -> &WorkerEntry {
        &self.inner.workers[self.idx]
    }
}

// ===== impl State =====

impl State {
    #[inline]
    fn new() -> State {
        let ret = State(MAX_WORKERS << STACK_SHIFT);

        debug_assert!(ret.is_running());
        debug_assert_eq!(ret.sleeper_head(), MAX_WORKERS);

        ret
    }

    #[inline]
    fn lifecycle(&self) -> usize {
        self.0 & LIFECYCLE_MASK
    }

    #[inline]
    fn is_running(&self) -> bool {
        self.lifecycle() == RUNNING
    }

    #[inline]
    fn sleeper_head(&self) -> usize {
        (self.0 & STACK_MASK) >> STACK_SHIFT
    }

    #[inline]
    fn set_sleeper_head(&mut self, val: usize) {
        // The ABA guard protects against the ABA problem w/ treiber stacks
        let aba_guard = ((self.0 >> ABA_GUARD_SHIFT) + 1) & ABA_GUARD_MASK;

        self.0 = (aba_guard << ABA_GUARD_SHIFT) |
            (val << STACK_SHIFT) |
            self.lifecycle();
    }
}

impl From<usize> for State {
    fn from(src: usize) -> Self {
        State(src)
    }
}

impl From<State> for usize {
    fn from(src: State) -> Self {
        src.0
    }
}

// ===== impl WorkerEntry =====

impl WorkerEntry {
    fn new() -> Self {
        WorkerEntry {
            state: AtomicUsize::new(WorkerState::default().into()),
            next_sleeper: UnsafeCell::new(0),
            deque: Deque::new(),
            inbound: task::Queue::new(),
            park_mutex: Mutex::new(()),
            park_condvar: Condvar::new(),
        }
    }

    #[inline]
    fn submit_internal(&self, task: Task) {
        self.push_internal(task);
    }

    /// Submits a task to the worker. This assumes that the caller is external
    /// to the worker. Internal submissions go through another path.
    ///
    /// Returns `false` if the worker needs to be spawned.
    fn submit_external(&self, task: Task, mut state: WorkerState) -> bool {
        // Push the task onto the external queue
        self.push_external(task);

        loop {
            let mut next = state;
            next.notify();

            let actual = self.state.compare_and_swap(
                state.into(), next.into(),
                AcqRel).into();

            if state == actual {
                break;
            }

            state = actual;
        }

        match state.lifecycle() {
            WORKER_SLEEPING => {
                // The worker is currently sleeping, the condition variable must
                // be signaled
                self.wakeup();
                true
            }
            WORKER_SHUTDOWN => false,
            _ => true,
        }
    }

    #[inline]
    fn push_external(&self, task: Task) {
        self.inbound.push(task);
    }

    #[inline]
    fn push_internal(&self, task: Task) {
        unsafe {
            self.deque.push(task);
        }
    }

    #[inline]
    fn wakeup(&self) {
        let _lock = self.park_mutex.lock().unwrap();
        self.park_condvar.notify_one();
    }

    #[inline]
    fn next_sleeper(&self) -> usize {
        unsafe { *self.next_sleeper.get() }
    }

    #[inline]
    fn set_next_sleeper(&self, val: usize) {
        unsafe { *self.next_sleeper.get() = val; }
    }
}

// ===== impl WorkerState =====

impl WorkerState {
    /// Returns true if the worker entry is pushed in the sleeper stack
    fn is_pushed(&self) -> bool {
        self.0 & PUSHED_MASK == PUSHED_MASK
    }

    fn set_pushed(&mut self) {
        self.0 |= PUSHED_MASK
    }

    fn is_notified(&self) -> bool {
        match self.lifecycle() {
            WORKER_NOTIFIED | WORKER_SIGNALED => true,
            _ => false,
        }
    }

    fn lifecycle(&self) -> usize {
        (self.0 & WORKER_LIFECYCLE_MASK) >> WORKER_LIFECYCLE_SHIFT
    }

    fn set_lifecycle(&mut self, val: usize) {
        self.0 = (self.0 & !WORKER_LIFECYCLE_MASK) |
            (val << WORKER_LIFECYCLE_SHIFT)
    }

    fn is_signaled(&self) -> bool {
        self.lifecycle() == WORKER_SIGNALED
    }

    fn notify(&mut self) {
        if self.lifecycle() != WORKER_SIGNALED {
            self.set_lifecycle(WORKER_NOTIFIED)
        }
    }
}

impl Default for WorkerState {
    fn default() -> WorkerState {
        // All workers will start pushed in the sleeping stack
        WorkerState(PUSHED_MASK)
    }
}

impl From<usize> for WorkerState {
    fn from(src: usize) -> Self {
        WorkerState(src)
    }
}

impl From<WorkerState> for usize {
    fn from(src: WorkerState) -> Self {
        src.0
    }
}

// ===== impl Callback =====

impl Callback {
    fn new<F>(f: F) -> Self
        where F: Fn() + Send + Sync + 'static
    {
        Callback { f: Arc::new(f) }
    }

    pub fn call(&self) {
        (self.f)()
    }
}

impl fmt::Debug for Callback {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Fn")
    }
}
