# 0.5.2 (2020-11-6)

- Optimize log replication.
- Fix some issues with membership change.
- Remove Sync constraint from SnapshotStream.
- Implement FileSnapshot.

# 0.5.1 (2020-10-30)

- Ractor out lol-test as a crate.
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
