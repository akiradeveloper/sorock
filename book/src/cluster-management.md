# Cluster Management

## Single server change approach

There are two known approaches to change the cluster membership.
One is by joint consensus and the other is what is called **single server change approach**.
Sorock implements the second one because the thesis author recommends it.

This approach exploits the log replication mechanism in membership change
and therefore can be implemented as an extension of the normal log replication
by defining `AddServer` and `RemoveServer` commands.
From the admin client, the following API will add or remove a Raft process in the cluster.

```proto
message AddServerRequest {
  uint32 shard_index = 1;
  string server_id = 2;
}

message RemoveServerRequest {
  uint32 shard_index = 1;
  string server_id = 2;
}
```

## Cluster bootstrapping

In Raft, any command is directed to the leader.
Now the question is how to add a Raft process to the empty cluster.

The answer is so-called **cluster bootstrapping**.
On receiving the AddServer command and the receiving node evaluates the cluster is empty, 
the node tries to form a new cluster with solely itself and immediately becomes a leader as a result of a self election.