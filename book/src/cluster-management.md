# Cluster Management

## Single server change approach

There are two approaches to membership change.
One is by joint consensus and the other is what is called **single server change approach**.
sorock implements the second one.

This approach exploits the log replication mechanism in membership change
and therefore can be implemented as an extension of the normal log replication
by defining AddServer and RemoveServer commands.
From the admin client, the following API can add or remove a Raft process in the cluster.

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
So the question is how to add a Raft process to the empty cluster.

The answer is so-called **cluster bootstrapping**.
On receiving the AddServer command and the receiving node recognizes the cluster is empty, 
the node tries to form a new cluster with itself and immediately becomes a leader as a result of a self election.