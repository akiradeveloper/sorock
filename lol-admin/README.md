# lol-admin

A toolset to add/remove cluster nodes.

## How to use

lol-admin $receiver

followed by sub command.

## Sub command

### init-cluster

A special operation to the first node. This operation is to 
form the initial cluster that is consist of the node only.

### add-server $addr / remove-server $addr

Add or remove new server which is not yet in the cluster.
Once the request is responded with OK, then the operation is committed.

### cluster-info

Query the cluster membership and the current leader.

### timeout-now

Force the receiver node to start election. This operation is useful to change the leader node to some designated node in the cluster.