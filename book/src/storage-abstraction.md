# Storage Abstraction

`RaftStorage` is the abstraction of the backing storage.
Conceptually, it includes the recent vote, log entries and snapshot tags. Every snapshot entry has a corresponding snapshot tag that points to actual snapshot resource.

By default, lol provides two implementations: one is in-memory type using BTreeMap and the another is persistent type using RocksDB. In most cases, you don't need to implement your own but choose from these two.

Some other Raft libraries like async-raft defines one thick abstraction that kind of mixes RaftApp and RaftStorage but I went the different way because these two can be cleanly separated in my architecture and it is more user-friendly because only thing user needs to implement is `RaftApp`.