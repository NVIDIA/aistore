The CLI allows users to interact with AIStore [Xactions](../../../xaction/README.md).

## Start xaction

`ais start xaction XACTION_NAME [BUCKET_NAME]`

Start xaction(s). Some xactions require a bucket name to execute.
The second argument is used to determine the bucket name if it is required.

### Examples

#### Start cluster-wide LRU

Starts LRU xaction on all nodes

```console
$ ais start xaction lru
Started "lru" xaction.
```

## Stop xaction

`ais stop xaction XACTION_NAME|all [BUCKET_NAME]`

Stop xaction(s). If the first argument is `all`, all xactions are stopped.
The second argument is used to determine the bucket name if it is required.

### Examples

#### Stop cluster-wide LRU

Stops currently running LRU xaction.

```console
$ ais stop xaction lru
Stopped "lru" xaction.
```

## Show xaction stats

`ais show xaction [XACTION_NAME] [BUCKET_NAME]`

Display details about `XACTION_NAME` xaction. If no arguments are given, displays details about all xactions.
The second argument is used to determine the bucket name if it is required.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output details in JSON format | `false` |
| `--all-items` | `bool` | If set, additionally displays old, finished xactions | `false` |
| `--active` | `bool` | If set, displays only running xactions | `false` |
| `--verbose` `-v` | `bool` | If set, displays extended information about xactions where available | `false` |

Certain extended actions have additional CLI. In particular, rebalance stats can also be displayed using the following command:

`ais show rebalance`

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh [N]` | `string` | watch the rebalance until it finishes or CTRL-C is pressed. Display the current stats every N seconds, where N ends with time suffix: s, m. If N is not defined it prints stats every 1 second | `1s` |

Output of this command differs from the generic xaction output.

## Wait for xaction

`ais wait xaction XACTION_NAME [BUCKET_NAME]`

Wait for the `XACTION_NAME` xaction to finish.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh rate | `1s` |
