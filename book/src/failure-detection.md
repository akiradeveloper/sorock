# Leader failure detection

Typical Raft implementation uses static election timeout to detect leader failure however, this strongly assumes that latency between nodes is uniform and measurable in prior of deployment. The assumption is broken when you go out to cloud.

To solve this, lol uses **adaptive failure detection** algorithm called Phi-Accurual detection algorithm. The concept is very simple: the history of heartbeats is given and assume the intervals are distributed normally then we are able to calculate the possiblity of getting another heartbeat in the future.

With this feature, applications based on lol can be deployed in cloud where the latency between nodes may be difference and unstable.