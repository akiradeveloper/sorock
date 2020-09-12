# lol-monitor

A tui software to monitor the cluster status such as
membership, replication progress, server health, etc.

## How to launch

lol-monitor $receiver

$receiver should be one of the node in the cluster.

After lol-monitor launches and successfully connected to the cluster,
the connection is maintained unless all node in the cluster is removed.
This is because lol-monitor tracks the membership information periodically.

## Key command

- q: quit