#![feature(test)]

extern crate futures;
extern crate futures_pool;
extern crate futures_cpupool;
extern crate num_cpus;
extern crate test;

const ITER: usize = 1_000;
const TASKS_PER_CPU: usize = 50;

mod us {
    use futures::{task, Async};
    use futures::future::{self, Executor};
    use futures_pool::*;
    use num_cpus;
    use test;
    use std::sync::mpsc;

    #[bench]
    fn basic(b: &mut test::Bencher) {
        let (sched_tx, _scheduler) = Pool::new();
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::ITER;
                let tx = tx.clone();

                sched_tx.execute(future::poll_fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Ok(Async::Ready(()))
                    } else {
                        // Notify the current task
                        task::current().notify();

                        // Not ready
                        Ok(Async::NotReady)
                    }
                })).ok().unwrap();
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}

// In this case, CPU pool completes the benchmark faster, but this is due to how
// CpuPool currently behaves, starving other futures. This completes the
// benchmark quickly but results in poor runtime characteristics for a thread
// pool.
//
// See alexcrichton/futures-rs#617
//
mod cpupool {
    use futures::{task, Async};
    use futures::future::{self, Executor};
    use futures_cpupool::*;
    use num_cpus;
    use test;
    use std::sync::mpsc;

    #[bench]
    fn basic(b: &mut test::Bencher) {
        let pool = CpuPool::new(num_cpus::get());
        let tasks = super::TASKS_PER_CPU * num_cpus::get();

        let (tx, rx) = mpsc::sync_channel(tasks);

        b.iter(move || {
            for _ in 0..tasks {
                let mut rem = super::ITER;
                let tx = tx.clone();

                pool.execute(future::poll_fn(move || {
                    rem -= 1;

                    if rem == 0 {
                        tx.send(()).unwrap();
                        Ok(Async::Ready(()))
                    } else {
                        // Notify the current task
                        task::current().notify();

                        // Not ready
                        Ok(Async::NotReady)
                    }
                })).ok().unwrap();
            }

            for _ in 0..tasks {
                let _ = rx.recv().unwrap();
            }
        });
    }
}
