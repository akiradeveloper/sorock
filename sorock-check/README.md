# sorock-check

Trouble shooting a Raft cluster by visualizing the cluster and the log progresses.

https://github.com/user-attachments/assets/9aff6794-778b-48fa-bfbd-838e63b3e5c8

## Usage

`sorock-check connect $URL $SHARD_ID`. (e.g. `sorock-check connect http://node5:50051 34`)

Once connected to any node in a cluster,
the program will automatically connect to all the nodes in the cluster.
