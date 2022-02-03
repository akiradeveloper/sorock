# Single-server changes

There are two methods to change the cluster membership in Raft: one is by joint concensus algorithm and the another is single-server changes.

In the initial Raft research paper, joint consensus algorithm was used to change the membership but later the author found a flaw in joint consensus algorithm, and the lastest dissertation uses single-server changes. lol uses this.

So what is single-server changes? This algorithm adds or removes only one server at a time.

These operations are represented as special requests (`AddServer` and `RemoveServer`) and it turns into `ClusterConfiguration` command in the log which is then replicated to the cluster as well as normal commands.

The rational behind this is a single-server change always share the majority nodes between before and after the change.
The following figure from Raft dissertation describes a case of adding one node.

![](images/single-server-changes.png)

So the next leader always know the latest membership and the consensus on membership change will never be lost.