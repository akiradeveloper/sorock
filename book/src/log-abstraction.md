# Log Abstraction

`RaftStorage` is the abstraction of the log storage.
Conceptually, it includes the recent vote and the log entries. Every snapshot entry is guaranteed to be have the corresponding snapshot resource because they are always committed after saving the corresponding resource.

By default, lol provides some implementations (memory, file and rocksdb). Normally, you can choose the best implementation from these three.

Some other Raft libraries like async-raft defines one huge abstraction that kind of mixes RaftApp and RaftStorage but I went the different way because these two can be cleanly separated in my architecture and it is more user-friendly because only thing user needs to implement is `RaftApp`.