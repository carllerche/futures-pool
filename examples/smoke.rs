extern crate futures;
extern crate futures_pool;
extern crate tokio_timer;
extern crate env_logger;

use futures::*;
use futures::sync::oneshot::spawn;
use futures_pool::*;

use tokio_timer::Timer;

use std::thread;
use std::time::Duration;

pub fn main() {
    let _ = ::env_logger::init();

    let timer = Timer::default();
    {
        let (tx, _scheduler) = Pool::new();

        let fut = timer.interval(Duration::from_millis(300))
            .for_each(|_| {
                println!("~~~~~ Hello ~~~");
                Ok(())
            })
            .map_err(|_| unimplemented!());

        spawn(fut, &tx).wait().unwrap();
    }

    thread::sleep(Duration::from_millis(100));
}
