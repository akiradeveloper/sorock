# Heartbeat Multiplexing

In Raft, leader is responsible for periodically sending heartbeats to every followers to maintain the leadership.
In Multi-Raft, without any optimization, the load from the the heartbeats will be non-negligible.
To address this problem, we can make an optimization to reduce the heartbeat RPCs by multiplexing the heartbeats from different shards.

## Problem of naive implementation

In the following diagram, we have two nodes (Node1 and Node2) and there are two shards.
A Process with an asterisk (*) is a leader: P1 is sending heartbeats to the P3 and P2 to P4 as well.
So there are two RPC messages sent from P1 to P3 and P2 to P4 independently.

```mermaid
graph LR
  subgraph Node1
    P1(P1*)
    P2(P2*)
  end
  subgraph Node2
    P3(P3)
    P4(P4)
  end
  P1 -->|heartbeat| P3
  P2 --> P4
```

## Multiplexing heartbeats

To reduce the number of RPCs to only one, we will install multiplexer (Mux) and demultiplexer (Demux) in the nodes.
The two heartbeats are buffered in the multiplexer and sent in a **batched RPC** to the destination node.
In the destination node, the demultiplexer will split the batched RPC into individual heartbeats
and send them to the corresponding processes.

```mermaid
graph LR
  subgraph Node1
    P1(P1*)
    P2(P2*)
    MUX(Mux)
  end
  subgraph Node2
    DEMUX(Demux)
    P3(P3)
    P4(P4)
  end
  P1 --> MUX
  P2 --> MUX
  MUX --> DEMUX
  DEMUX --> P3
  DEMUX --> P4
```

In this case the reduction rate is 2.

## Math: Reduction rate

What is the reduction rate in general?
Let's do some easy math.

Let's consider there are N nodes and L shards.
Each shard have K replication and leader processes are
balanced among the nodes.

In this case,
the total number of heartbeats sent in a period is LK.
And the total directed paths connecting two nodes is N(N-1) and
these heartbeats are evenly attributed to these paths.
Therefore, the number of heartbeats sent in each path is LK/(N(N-1))
and this is the solution.
For example, if there are 5 nodes and 1000 shards with 3 replication,
the reduction rate is 150.
