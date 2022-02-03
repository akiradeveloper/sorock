# Snapshot Types (Copy and Fold)

The most difficult part of Raft implementation is how to deal with the snapshot. If you are a Raft library implementator like me, you will agree with this.

From the word "snapshot" you may imagine it is a copy (or light-weight snapshot) of the state machine which is provided by `RaftApp`. Yes, lol calls this type of snapshotting **Copy snapshot**. When `RaftApp` returns a Copy snapshot from `process_write` this snapshot is granted as a snapshot up to the current apply index.

From a different point of view, snapshot can be seen as recomputing the log entries up to some point in time. lol call this type of snapshot **Fold snapshot**.

lol supports both types of snapshot: Copy snapshot and Fold snapshot.

The comparison between these two is copy cost + overhead VS recomputation cost: Copy snapshot needs to copy the current snapshot in `process_write` so copy cost is charged (RocksDB or [dm-thin](https://www.kernel.org/doc/Documentation/device-mapper/thin-provisioning.txt)'s snapshot is not even zero-cost) and the operation spends extra time. Fold snapshot on the other hands, can be executed in parallel with `process_write` but costs from recomputation.