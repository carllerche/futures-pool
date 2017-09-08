#![feature(test)]

extern crate futures;
extern crate futures_pool;
extern crate test;

use futures::future::{self, Executor};
use futures_pool::*;
use std::sync::mpsc;

const ITER: usize = 20_000;

#[bench]
fn chained_spawn(b: &mut test::Bencher) {
    let (sched_tx, _scheduler) = Pool::new();

    fn spawn(sched_tx: Sender, res_tx: mpsc::Sender<()>, n: usize) {
        if n == 0 {
            res_tx.send(()).unwrap();
        } else {
            let sched_tx2 = sched_tx.clone();
            sched_tx.execute(future::lazy(move || {
                spawn(sched_tx2, res_tx, n - 1);
                Ok(())
            })).ok().unwrap();
        }
    }

    b.iter(move || {
        let (res_tx, res_rx) = mpsc::channel();

        spawn(sched_tx.clone(), res_tx, ITER);
        res_rx.recv().unwrap();
    });
}
