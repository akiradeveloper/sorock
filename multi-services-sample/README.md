# multi-services-sample

When you build a Raft app, you often want to add services outside of lol's Raft capabilities.

For example, when you build a distributed KVS on Raft consensus algorithm, PUT and GET operations are implemented as log-aware Raft operations.

But you may want to implement an operation that queries the state of the application like remaining storage capacity. Such operation isn't relevant to Raft consensus so should be implemented outside of Raft.

In this case, you can choose to implement your own `Service` and stack it onto the Raft service. Tonic appropriately routes your request to the target service.

This crate is a PoC for this method.

![](multi-services.png)