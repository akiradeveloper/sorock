# lol-admin

A toolset to add/remove cluster nodes.

## How to use

lol-admin $receiver

followed by sub command.

## Sub command

### add-server $addr / remove-server $addr

Add or remove new server which is not yet in the cluster.
Once the request is responded with OK, then the operation is committed.

### cluster-info

Query the cluster membership and the current leader.

### timeout-now

Force the receiver node to start election. This operation is useful to change the leader node to some designated node in the cluster.