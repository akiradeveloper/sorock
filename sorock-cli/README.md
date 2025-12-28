# Sorock-CLI

## Sync

Synchronize shard mapping among nodes.

**Example**: `sorock-cli sync http://node1:50000 http://node2:50000 http://node3:50000 http://node4:50000`

The command above syncs mapping configurations among the four nodes.

## Remap

Change shard configurations.

**Example** `echo "14 [1] [2]* [4]" | sorock-cli remap --node-list http://node1:50000 http://node2:50000 http://node3:50000 http://node4:50000`

The command above changes the configurtion of shard (id=14) so node1, node2 and node4 forms the cluster and set node2 as leader.
You can change configurations of multiple shards at the same time by giving multiline stdio to the command.

## Monitor

Visualize a cluster and the log progress.

https://github.com/user-attachments/assets/9aff6794-778b-48fa-bfbd-838e63b3e5c8

**Example**: `sorock-cli monitor connect http://node2:50000`