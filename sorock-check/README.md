# sorock-check

Trouble shooting a distributed nodes is a very difficult task.

In Raft, finding a node which is slow in synchronization is the first step 
and visualizing the log states is the quickest way to do this. Therefore, we need a tool like this.

https://github.com/user-attachments/assets/9aff6794-778b-48fa-bfbd-838e63b3e5c8

## Usage

`sorock-check connect $URL $SHARD_ID`. (e.g. `sorock-check connect http://my-cluster-node-5:50051 34`)

Once connected to any of the nodes in the cluster,
the monitor will connect to all the nodes in the cluster and start fetching the log states periodically.

