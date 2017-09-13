extern crate env_logger;
extern crate futures;
extern crate futures_pool;

use futures::future::{Future, Executor, lazy};
use futures_pool::*;

use std::cell::Cell;
use std::sync::mpsc;

thread_local!(static FOO: Cell<u32> = Cell::new(0));

// TODO: Test this w/ force_shutdown
#[test]
fn natural_shutdown_simple_futures() {
    let _ = ::env_logger::init();

    FOO.with(|f| {
        f.set(1);

        let (tx, pool) = Pool::new();

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
                println!("!!!!!!!!!!!!!!!!!!!!!! ");
                // Makes sure this runs on a worker thread
                FOO.with(|f| assert_eq!(f.get(), 0));

                t.send("two").unwrap();
                Ok(())
            })).unwrap();
            rx
        };

        drop(tx);

        println!(" ++ try recv a");
        assert_eq!("one", a.recv().unwrap());
        println!(" ++ try recv b");
        assert_eq!("two", b.recv().unwrap());

        println!("~~~~~~~~~~~~~~");

        // Wait for the pool to shutdown
        pool.wait().unwrap();
    });
}
