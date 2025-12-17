# Implementation of churn in Testground
This is a test plan, that simulates a churn model on an IPFS network.

## foreign code
This projekt uses foreign code. The foreign code in this directory is marked. Sources are:
- https://github.com/testground/testground/blob/master/plans/network/pingpong.go the specific commit was 8629aa2
- https://github.com/testground/testground/blob/master/plans/network/main.go the specific commit was b21e2d1
- https://github.com/ipfs/kubo/blob/master/docs/examples/kubo-as-a-library/main.go the specific commit was Commit 58c2939

The basic server code was inspired by:
- https://www.youtube.com/watch?v=qJQrrscB1-4

## codebase
The code in this directory, that is not marked as belonging to some other source, was written by the maintainer (Thorwin Bergholz).

## important for testplan functionality
The time this test takes is often quite long even on default settings. If used include the .env.toml in $TESTGROUND_HOME as described in: 
- https://docs.testground.ai/master/#/getting-started
or use the Testground version of this fork and don't change the $TESTGROUND_HOME directory after installation. If this is not done the test will often hit the task timeout and fail with context deadline exceeded.

The .env.toml can be found under:
- https://github.com/KIHumy/testground-with-node-churn/blob/master/.env.toml