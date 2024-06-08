# Batched Write

In multi-raft, multiple shards process write requests. Conceptually, each shard maintains its own log for entry insertion.

Having a physically independent log for each shard isn't efficient as each write requires a transaction to persist the data on the storage.

However, an optimization technique called "batching" can be used. Here, each shard maintains a virtual log, and the entries are temporarily queued in a shared queue. These queued entries are then processed in a single transaction, reducing the number of transactions.

This approach often presents a throughput versus latency dilemma. However, this implementation increases throughput without sacrificing latency.

```mermaid
graph LR
  CLI(Client)

  subgraph P1
    T1(redb::Table)
  end
  subgraph P2
    T2(redb::Table)
  end
  subgraph P3
    T3(redb::Table)
  end

  subgraph Reaper
    Q(Queue)
  end
  DB[(redb::Database)]

  CLI -->|entry| T1
  CLI --> T2
  CLI --> T3
  T1 -->|lazy entry| Q
  T2 --> Q
  T3 --> Q
  Q -->|transaction| DB

```