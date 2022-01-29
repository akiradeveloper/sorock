# lol-admin

A toolset to add/remove cluster nodes.

## Usage

```
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
    config
    help              Print this message or the help of the given subcommand(s)
    remove-server
    status
    timeout-now
    tunable-config
```

## Docker

```
docker build -t lol-admin -f Dockerfile ..
docker run --network="host" lol-admin http://localhost:3000 cluster-info
```
