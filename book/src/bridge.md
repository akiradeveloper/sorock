# RaftApp Bridge

Usually, `RaftApp` is implemented in Rust as well as the lol itself to the maximum performance. However, sometimes it is demanded to write app in other language like Go and Java.

You have two choices: one is to search for a Raft library in the language and the other is manage to connect with lol via bridge, which I recommend you to do so
because lol is the most sophisticated Raft library.

![Bridge](images/bridge.png)

To use the bridge, you need to write a gRPC server that is to communicate with the bridge. As the protocol buffer is language-independent, you can write your app in any language. Two types of transport are currently supported: TCP and UDS.