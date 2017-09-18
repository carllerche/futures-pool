extern crate env_logger;
extern crate futures;
extern crate futures_pool;

use futures::{Poll, Sink, Stream, Async};
use futures::future::{Future, Executor, lazy};
use futures_pool::*;

use std::cell::Cell;
use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT};
use std::sync::atomic::Ordering::Relaxed;
use std::time::Duration;

thread_local!(static FOO: Cell<u32> = Cell::new(0));

#[test]
fn natural_shutdown_simple_futures() {
    let _ = ::env_logger::init();

    for _ in 0..1_000 {
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

    for _ in 0..1_000 {
        let num_inc = Arc::new(AtomicUsize::new(0));
        let num_dec = Arc::new(AtomicUsize::new(0));
        let num_drop = Arc::new(AtomicUsize::new(0));

        struct Never(Arc<AtomicUsize>);

        impl Future for Never {
            type Item = ();
            type Error = ();

            fn poll(&mut self) -> Poll<(), ()> {
                Ok(Async::NotReady)
            }
        }

        impl Drop for Never {
            fn drop(&mut self) {
                self.0.fetch_add(1, Relaxed);
            }
        }

        let a = num_inc.clone();
        let b = num_dec.clone();

        let (tx, mut pool) = Pool::builder()
            .after_start(move || {
                a.fetch_add(1, Relaxed);
            })
            .before_stop(move || {
                b.fetch_add(1, Relaxed);
            })
            .build();

        tx.execute(Never(num_drop.clone())).unwrap();

        pool.force_shutdown();

        // Wait for the pool to shutdown
        pool.wait().unwrap();

        // Assert that only a single thread was spawned.
        let a = num_inc.load(Relaxed);
        assert!(a >= 1);

        // Assert that all threads shutdown
        let b = num_dec.load(Relaxed);
        assert_eq!(a, b);

        // Assert that the future was dropped
        let c = num_drop.load(Relaxed);
        assert_eq!(c, 1);
    }
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
        .before_stop(move || {
            // There could be multiple threads here
            let _ = t.lock().unwrap().send(());
        })
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

#[test]
fn many_oneshot_futures() {
    const NUM: usize = 10_000;

    let _ = ::env_logger::init();

    for _ in 0..50 {
        let (tx, pool) = Pool::new();
        let cnt = Arc::new(AtomicUsize::new(0));

        for _ in 0..NUM {
            let cnt = cnt.clone();
            tx.execute(lazy(move || {
                cnt.fetch_add(1, Relaxed);
                Ok(())
            })).unwrap();
        }

        // Wait for the pool to shutdown
        pool.wait().unwrap();

        let num = cnt.load(Relaxed);
        assert_eq!(num, NUM);
    }
}

#[test]
fn many_multishot_futures() {
    use futures::sync::mpsc;

    const CHAIN: usize = 200;
    const CYCLES: usize = 5;
    const TRACKS: usize = 50;

    let _ = ::env_logger::init();

    for _ in 0..50 {
        let (pool_tx, pool) = Pool::new();

        let mut start_txs = Vec::with_capacity(TRACKS);
        let mut final_rxs = Vec::with_capacity(TRACKS);

        for _ in 0..TRACKS {
            let (start_tx, mut chain_rx) = mpsc::channel(10);

            for _ in 0..CHAIN {
                let (next_tx, next_rx) = mpsc::channel(10);

                let rx = chain_rx
                    .map_err(|e| panic!("{:?}", e));

                // Forward all the messages
                pool_tx.execute(next_tx
                    .send_all(rx)
                    .map(|_| ())
                    .map_err(|e| panic!("{:?}", e))
                ).unwrap();

                chain_rx = next_rx;
            }

            // This final task cycles if needed
            let (final_tx, final_rx) = mpsc::channel(10);
            let cycle_tx = start_tx.clone();
            let mut rem = CYCLES;

            pool_tx.execute(chain_rx.take(CYCLES as u64).for_each(move |msg| {
                rem -= 1;
                let send = if rem == 0 {
                    final_tx.clone().send(msg)
                } else {
                    cycle_tx.clone().send(msg)
                };

                send.then(|res| {
                    res.unwrap();
                    Ok(())
                })
            })).unwrap();

            start_txs.push(start_tx);
            final_rxs.push(final_rx);
        }

        for start_tx in start_txs {
            start_tx.send("ping").wait().unwrap();
        }

        for final_rx in final_rxs {
            final_rx.wait().next().unwrap().unwrap();
        }

        // Shutdown the pool
        pool.wait().unwrap();
    }
}
