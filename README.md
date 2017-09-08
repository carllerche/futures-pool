# Futures pool

A library for scheduling execution of futures concurrently across a pool of
threads.

**Note**: This library isn't quite ready for use.

[![Travis Build Status](https://travis-ci.org/carllerche/futures-pool.svg?branch=master)](https://travis-ci.org/carllerche/futures-pool)

### Why not Rayon?

Rayon is designed to handle parallelizing single computations by breaking them
into smaller chunks. The scheduling for each individual chunk doesn't matter as
long as the root computation completes in a timely fashion. In other words,
Rayon does not provide any guarantees of fairness with regards to how each task
gets scheduled.

On the other hand, `futures-pool` is a general purpose scheduler and attempts to
schedule each task fairly. This is the ideal behavior when scheduling a set of
unrelated tasks.

### Why not futures-cpupool?

It's 10x slower.

## Usage

To use `futures-pool`, first add this to your `Cargo.toml`:

```toml
[dependencies]
futures-pool = { git = "https://github.com/carllerche/futures-pool" } # Soon on crates.io
```

Next, add this to your crate:

```rust
extern crate futures_pool;

fn main() {
    // ...
}
```
## Examples

```rust
extern crate futures;
extern crate futures_pool;

use futures::*;
use futures::sync::oneshot;
use futures_pool::*;

pub fn main() {
    let (tx, _pool) = Pool::new();

    let res = oneshot::spawn(future::lazy(|| {
        println!("Running on the pool");
        Ok::<_, ()>("complete")
    }), &tx);

    println!("Result: {:?}", res.wait());
}
```

## License

`futures-pool` is primarily distributed under the terms of both the MIT license
and the Apache License (Version 2.0), with portions covered by various BSD-like
licenses.

See LICENSE-APACHE, and LICENSE-MIT for details.
