# Raft Process

The core of multi-raft is Raft process.
Each multi-raft server has one or more Raft processes.

To implement multi-raft,
Sorock implements Raft process as it is fully agnostic to
detailed node communications through gRPC.
Since the Raft process doesn't know about the IO,
we call it **Pure Raft**.

![](images/raft-process.png)

To make Raft process to communicate with other Raft processes
through network, `RaftHandle` must be provided.
Everything about actual on-network communication is encapsulated under `RaftHandle`.

```rust
impl RaftProcess {
    pub async fn new(
        app: impl RaftApp,
        storage: &RaftStorage,
        handle: RaftHandle,
    ) -> Result<Self> {
```