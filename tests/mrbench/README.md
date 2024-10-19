# Mr. Bench

mrbench is a benchmark program for sorock.

## Pseudo Code

```python
cluster = create_cluster(num_nodes, num_shards)
while elapsed < du:
  io_batch = []
  for shard_id in [0, num_shards)
    io_batch <- num_batch_writes * cluster[0].shard[shard_id].write(io_size)
    io_batch <- num_batch_reads * cluster[0].shard[shard_id].read()
  par_execute(io_batch)
```

## Example

`mrbench -n 3 -p 10 -t 10s -w 10 -r 90 --io-size=1024 --enable-console`

## Options

| Name | Default | Example | Description |
| - | - | - | - | 
| num-nodes (n) | 1 | 3 | Number of nodes in the cluster |
| num-shards (p)  | 1 | 100 | Number of shards in each node |
| du (t) | 1s | 500ms | IO duration |
| n-batch-writes (w) | 1 | 30 | Number of writes in a IO batch |
| n-batch-reads (r) | 0 | 70 | Number of reads in a IO batch |
| io-size | 1 | 1024 | Write IO size in B |
| enable-console | false | | Enable tokio-console |
| compaction-compaction | 1 | | The cycles to be done between compactions |