# Leadership

## Command redirection

Raft is a leader-based consensus algorithm.
Only a single leader can exist in the cluster at a time and
all commands are directed to the leader.

In sorock, if the receiving Raft process isn't the leader,
the command is redirected to the leader.

## Adaptive leader failure detection

Detecting the leader's failure is a very important issue in Raft algorithm.
The naive implementation can send heartbeats to the followers periodically and
followers can detect the leader's failure by timeout.
However, this approach requires the heartbeat interval and the timeout duration
to be set properly before deployment. This brings another complexity.
Not only that, these times can't be fixed to a single value when
the distance between nodes is heterogeneous such as geo-distributed environment.

To solve this problem, sorock uses an adaptive failure detection algorithm called
**Phi accrual failure detector**.
With this approach, users are free from setting the timing parameters.

## Leadership transfer extension

In multi-raft, changing the cluster members is not a rare case.
An example of this case is rebalancing:
To balance the CPU/disk usage between nodes, Raft process may be 
moved to other nodes.

If the Raft process to be removed is the leader, the cluster will not have a
leader until a new leader is elected which causes downtime.

To mitigate this problem, the admin client can send a TimeoutNow command to
any remaining Raft process to forcibly start a new election (by promoting to a candidate in Raft term).

```proto
message TimeoutNow {
  uint32 shard_id = 1;
}
```