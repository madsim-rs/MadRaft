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
seed=1629626496
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

Happy coding and Good luck!

## License

Apache License 2.0
