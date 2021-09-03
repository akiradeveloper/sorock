# lol-admin

A toolset to add/remove cluster nodes.

## Usage

```
$ lol-admin --help
lol-admin

USAGE:
    lol-admin <DEST_ID> <SUBCOMMAND>

ARGS:
    <DEST_ID>

FLAGS:
    -h, --help       Print help information
    -V, --version    Print version information

SUBCOMMANDS:
    add-server
    cluster-info
    help              Print this message or the help of the given subcommand(s)
    remove-server
    timeout-now
    tunable-config
```

## Sub command

### add-server $addr / remove-server $addr

Add or remove new server which is not yet in the cluster.
Once the request is responded with OK, then the operation is committed.

### cluster-info

Query the cluster membership and the current leader.

### timeout-now

Force the receiver node to start election. This operation is useful to change the leader node to some designated node in the cluster.