# MadRaft

[![CI](https://github.com/madsys-dev/madraft/workflows/CI/badge.svg?branch=main)](https://github.com/madsys-dev/madraft/actions)

The labs of Raft consensus algorithm based on [MadSim](https://github.com/madsys-dev/madsim).

Some codes are derived from [MIT 6.824](http://nil.csail.mit.edu/6.824/2021/) and [PingCAP Talent Plan: Raft Lab](https://github.com/pingcap/talent-plan/tree/master/courses/dss/raft). Thanks for their excellent work!

## Key Features

* **Deterministic simulation**: Catch a rare bug and then reproduce it at any time you want. 
* **Discrete event simulation**: No time wasted on sleep. The full test can be completed in a few seconds.
* **Async**: The code is written in a fully async-style.

## Mission

Read the instructions from MIT 6.824: [Lab2](http://nil.csail.mit.edu/6.824/2021/labs/lab-raft.html), [Lab3](https://pdos.csail.mit.edu/6.824/labs/lab-kvraft.html), [Lab4](https://pdos.csail.mit.edu/6.824/labs/lab-shard.html).

Complete the code and pass all tests!

```sh
cargo test
```

## Note

If you use MadSim v0.1.*, make sure your rustc version < 1.72.0 by running the following commands:
```sh
rustup install 1.70.0
rustup default 1.70.0
```

## Tips

To run a part of the tests or a specific test:

```sh
cargo test 2a
cargo test initial_election_2a
```

If a test fails, you will see a seed in the output:

```
---- raft::tests::initial_election_2a stdout ----
thread 'raft::tests::initial_election_2a' panicked at 'expected one leader, got none', src/raft/tester.rs:91:9
note: run with `RUST_BACKTRACE=1` environment variable to display a backtrace
MADSIM_TEST_SEED=1629626496
```

Run the test again with the seed, and you will get exactly the same output:

```sh
MADSIM_TEST_SEED=1629626496 cargo test initial_election_2a
```

Enable logs to help debugging:

```sh
export RUST_LOG=madraft::raft=info
```

Run the test multiple times to make sure you solution can stably pass the test:

```sh
MADSIM_TEST_NUM=100 cargo test --release
```

### Ensure Determinism

Sometimes you may find that the test is not deterministic :(

Although the testing framework itself (MadSim) provides determinism, the entire system is not deterministic if your code introduces randomness.

Here are some tips to avoid randomness:

* Use `madsim::rand::rng` instead of `rand::thread_rng` to generate random numbers.
* Use `futures::select_biased` instead of `futures::select` macro.
* Do not **iterate** through a `HashMap`.

To make sure your code is deterministic, run your test with the following environment variable:

```sh
MADSIM_TEST_CHECK_DETERMINISTIC=1 cargo test
```

Your test will be run at least twice with the same seed. If any non-determinism is detected, it will panic as soon as possible.

Happy coding and Good luck!

## License

Apache License 2.0
