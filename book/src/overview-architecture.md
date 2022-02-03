# Overview of the architecture

This chapter describes the overview of the architecture.

![](images/overview.png)

`RaftCore` is the heart of the Raft system. It includes internal states and functions that typically produces side effects.

Around the `RaftCore`, there are many companion threads that indefinitely loops and sends messages to `RaftCore`.

How much threads are there? As of 0.7, there are 7 threads around the core. This architecure is much like typical operating system that daemon threads are orbiting around the kernel.

The last element of the system is `RaftApp`. 
As its name suggests, it is a user application which runs on the `RaftCore`. The responsibility of the `RaftApp` is said to be only two things:

1. Apply the messages to the state machine.
2. Make a new snapshot.

By connecting your `RaftApp` with `RaftCore`, you can build your own Raft process that interacts with other Raft processes to form a Raft cluster of your own.

If you are ready to implement your own `RaftApp`, API doc and the KVS implementation under `example/kvs/` would help.