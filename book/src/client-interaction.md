# Client Interaction

You can send application-defined commands to the Raft cluster.

Sorock distinguishes the command into two types: read-write (R/W) and read-only (R/O). 
The R/O command is called **query** and queries can be processed in parallel through the optimized path.

## R/W commnand

R/W command is a normal application command which is inserted into the log to be applied later.

You can send a R/W command to the cluster with this API.

```proto
message WriteRequest {
  uint32 shard_index = 1;
  bytes message = 2;
  string request_id = 3;
}
```

**request_id** is required to avoid doubly application.
Let's think about this scenario:

1. The client sends a R/W command to add 1 to the value.
2. The leader server replicates the command to the followers but crashes before application and response.
3. The client resends the command to a new leader after a timeout.
4. The result is adding 2 in total to the value while the expectation is only 1.

To avoid this issue, request_id is added to identify the commands.

## R/O command

The R/O command can bypass the log because it is safe to execute the query after the
commit index at time of query is applied. This is called **read_index** optimization.

You can send a R/O command to the cluster with the following API.

When `read_locally` is set to true, the request isn't proxied to the leader but
processed locally.

```proto
message ReadRequest {
  uint32 shard_index = 1;
  bytes message = 2;
  bool read_locally = 3;
}
```