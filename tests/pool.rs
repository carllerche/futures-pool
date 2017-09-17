extern crate env_logger;
extern crate futures;
extern crate futures_pool;

use futures::{Poll, Async};
use futures::future::{Future, Executor, lazy};
use futures_pool::*;

use std::cell::Cell;
use std::sync::mpsc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

thread_local!(static FOO: Cell<u32> = Cell::new(0));

#[test]
fn natural_shutdown_simple_futures() {
    let _ = ::env_logger::init();

    for i in 0..1_000 {
        static NUM_INC: AtomicUsize = ATOMIC_USIZE_INIT;
        static NUM_DEC: AtomicUsize = ATOMIC_USIZE_INIT;

        FOO.with(|f| {
            f.set(1);

            let (tx, pool) = Pool::builder()
                .after_start(|| {
                    NUM_INC.fetch_add(1, Relaxed);
                })
                .before_stop(|| {
                    NUM_DEC.fetch_add(1, Relaxed);
                })
                .build();

            let a = {
                let (t, rx) = mpsc::channel();
                tx.execute(lazy(move || {
                    // Makes sure this runs on a worker thread
                    FOO.with(|f| assert_eq!(f.get(), 0));

                    t.send("one").unwrap();
                    Ok(())
                })).unwrap();
                rx
            };

            let b = {
                let (t, rx) = mpsc::channel();
                tx.execute(lazy(move || {
                    // Makes sure this runs on a worker thread
                    FOO.with(|f| assert_eq!(f.get(), 0));

                    t.send("two").unwrap();
                    Ok(())
                })).unwrap();
                rx
            };

            drop(tx);

            assert_eq!("one", a.recv().unwrap());
            assert_eq!("two", b.recv().unwrap());

            // Wait for the pool to shutdown
            pool.wait().unwrap();

            // Assert that at least one thread started
            let num_inc = NUM_INC.load(Relaxed);
            assert!(num_inc > 0);

            // Assert that all threads shutdown
            let num_dec = NUM_DEC.load(Relaxed);
            assert_eq!(num_inc, num_dec);
        });
    }
}

#[test]
fn force_shutdown_drops_futures() {
    let _ = ::env_logger::init();

    static NUM_INC: AtomicUsize = ATOMIC_USIZE_INIT;
    static NUM_DEC: AtomicUsize = ATOMIC_USIZE_INIT;
    static NUM_DROP: AtomicUsize = ATOMIC_USIZE_INIT;

    struct Never;

    impl Future for Never {
        type Item = ();
        type Error = ();

        fn poll(&mut self) -> Poll<(), ()> {
            Ok(Async::NotReady)
        }
    }

    impl Drop for Never {
        fn drop(&mut self) {
            NUM_DROP.fetch_add(1, Relaxed);
        }
    }

    let (tx, mut pool) = Pool::builder()
        .after_start(|| {
            NUM_INC.fetch_add(1, Relaxed);
        })
        .before_stop(|| {
            NUM_DEC.fetch_add(1, Relaxed);
        })
        .build();

    tx.execute(Never).unwrap();

    pool.force_shutdown();

    // Wait for the pool to shutdown
    pool.wait().unwrap();

    // Assert that only a single thread was spawned.
    let num_inc = NUM_INC.load(Relaxed);
    assert_eq!(num_inc, 1);

    // Assert that all threads shutdown
    let num_dec = NUM_DEC.load(Relaxed);
    assert_eq!(num_inc, num_dec);

    // Assert that the future was dropped
    let num_drop = NUM_DROP.load(Relaxed);
    assert_eq!(num_drop, 1);
}

#[test]
fn thread_shutdown_timeout() {
    use std::sync::Mutex;

    let _ = ::env_logger::init();

    let (shutdown_tx, shutdown_rx) = mpsc::channel();
    let (complete_tx, complete_rx) = mpsc::channel();

    let t = Mutex::new(shutdown_tx);

    let (tx, pool) = Pool::builder()
        .keep_alive(Duration::from_millis(200))
        .before_stop(move || t.lock().unwrap().send(()).unwrap())
        .build();

    let t = complete_tx.clone();
    tx.execute(lazy(move || {
        t.send(()).unwrap();
        Ok(())
    })).unwrap();

    // The future completes
    complete_rx.recv().unwrap();

    // The thread shuts down eventually
    shutdown_rx.recv().unwrap();

    // Futures can still be run
    tx.execute(lazy(move || {
        complete_tx.send(()).unwrap();
        Ok(())
    })).unwrap();

    complete_rx.recv().unwrap();

    drop(pool);
}
