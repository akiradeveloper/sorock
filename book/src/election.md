# Leader Election

In Raft, leader has a special authority in the cluster.

The algorithm proves only one leader exists in one term and the leader is at least
"stronger" (I have no idea about better expression) than the majority of the cluster servers.
The principle is very simple however, there are non-trivial corner cases and some useful extensions.