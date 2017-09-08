extern crate futures;
extern crate futures_pool;
extern crate env_logger;

use futures::future::{self, Executor};
use futures_pool::*;

use std::sync::mpsc;

const ITER: usize = 2_000_000;
// const ITER: usize = 30;

fn chained_spawn() {
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

    loop {
        println!("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        let (res_tx, res_rx) = mpsc::channel();

        for _ in 0..10 {
            spawn(sched_tx.clone(), res_tx.clone(), ITER);
        }

        for _ in 0..10 {
            res_rx.recv().unwrap();
        }
    }
}

pub fn main() {
    let _ = ::env_logger::init();
    chained_spawn();
}
