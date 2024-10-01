# 0.10.2 (2024-3-13)

- Implement reflection.
- Use separate codegen instead of build.rs

# 0.10.1 (2024-3-11)

- Commit auto-generated code to work around docs.rs issue.

# 0.10.0 (2024-3-11)

- Fully re-architected.
- Implement Multi-Raft.
- Rename lol-core -> lolraft

# 0.9.5 (2022-12-8)

- Fix.

# 0.9.4 (2022-12-8)

- Work around for docs.rs issue: Commit auto-generated code. (#307)

# 0.9.3 (2022-12-8)

- Remove unnecessary flag: experimental_allow_proto3_optional. (#303)
- Conform to proto style guide. (#304)

# 0.9.2 (2022-12-8)

- Fix: Locking in snapshot queue. (#280)
- Fix: Avoid panic in GC. (#285)
- Publish `Ballot::new` for storage implementors. (#298)
- Dependency update: Tonic 0.8, RocksDB 0.19, etc.

# 0.9.1 (2022-2-12)

- Remove unnecessary copy in snapshot stream. (#269)
- Improve doc.

# 0.9.0 (2022-2-8)

- Remove the concept of snapshot tag. (#260)
- Remove lol-bridge.
- Remove lol-monitor.

# 0.8.0 (2022-2-3)

- Rework API design.

# 0.7.6 (2021-9-13)

- Deprecate snapshot-related functions in RaftStorage. (#237)

# 0.7.5 (2021-9-8)

- Fix doc.

# 0.7.4 (2021-9-8)

- Add atomic-counter example. (#213)
- Fix a bug in gateway. (#207)
- Add new APIs.

# 0.7.3 (2021-9-3)

- Implement new Gateway. (#199)
- Connection timeout in sending user data. (#202)
- Add ClusterInfo RPC.
- Dependency update.

# 0.7.2 (2021-4-1)

- Implement Tune message. (#173)
- Fix error handling. (#168)

# 0.7.1 (2021-2-2)

- Use JoinHandle#abort to abort the running thread. (#159)
- Add code coverage measurement. (#161)

# 0.7.0 (2021-1-25)

- Now based on Tokio 1.0. (#144)
- Stability Improvement: Introduce Pre-Vote phase. (#139)
- Use prost's new feature to generate Bytes as bytes. (#150)
- Performance Inprovement: Change internal serializer from MessagePack to Bincode. (#152)

# 0.6.3 (2021-1-7)

- Revert on-faliure log blocking which was introduced in 0.5.3 (@07465c5)
- Refactor code about cluster membership. (#133)
- Update documents.

# 0.6.2 (2020-12-10)

- Introduce new failure detector based on Phi Accrual Failure Detection algorithm. (#122)
- Heartbeat thread is maintained per follower node. (#114)
- The initial node immediately starts the initial election by sending TimeoutNow to itself. (#118)
- Fix critical bug in fold snapshot. (#124)
- Fix bug in election. Ballot should accept only one single writer. (#119)

# 0.6.1 (2020-12-2)

- Hide connection::Endpoint which wasn't my intention to expose.
- Fix tests

# 0.6.0 (2020-12-1)

- Clear separation between library and tonic infrastructure.
- RaftApp's apply_message now returns MakeSnapshot.
- Change Id's format to support TLS.

# 0.5.3 (2020-11-30)

- Insert adaptive wait after apply failure to avoid infinitely repeat failing calls.
- Log is blocked on apply failure to avoid inflation.

# 0.5.2 (2020-11-6)

- Optimize log replication.
- Fix some issues with membership change.
- Remove Sync constraint from SnapshotStream.
- Implement FileSnapshot.

# 0.5.1 (2020-10-30)

- Factor out lol-test as a crate.
- Wait for noop entry to be committed before appending new entries in the new term.
- Add AddServer/RemoveServer APIs to proto file. Deprecate InitCluster API.

# 0.5.0 (2020-10-23)

- RaftApp abstraction update: Notion of Snapshot Tag.
- Performance optimization: Command in log entry is now Bytes and it's applied to RaftApp in zero-copy.
- Error handling improved: RaftStorage now returns Result so callers can handle failures.
- Many fixes especially with persistent backend.
- Rename protoimpl to proto_compiled because it is ambiguous.

# 0.4.0 (2020-10-11)

- Introduce snapshot inventory: Application snapshot is now put in snapshot inventory and sent to other servers in stream.
- Snapshot type is generalized: Now it is not restricted to `Vec<u8>`.
- Change client interaction: ProcessReq/Rep type is defined.

# 0.3.0 (2020-10-6)

- Add apply_index to apply_message and install_snapshot so state machine can remember the last applied index to skip the previous messages after reboot.
- Implement copy snapshot. Now you can make a snapshot by either folding the log before the last applied index or returning snapshot from apply_message.
- Linking with RocksDB backend is now optional.

# 0.2.1 (2020-10-1)

- Implement RocksDB implementation of the RaftStorage.

# 0.2.0 (2020-9-26)

- Introduce RaftStorage abstraction.
