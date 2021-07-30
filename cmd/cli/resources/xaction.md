---
layout: post
title: XACTION
permalink: cmd/cli/resources/xaction
redirect_from:
 - cmd/cli/resources/xaction.md/
---

# Xaction (Job) management

Batch operations that may take many seconds (minutes, hours) to execute are called eXtended actions or *xactions*.

AIS *xactions* run asynchronously, have one of the enumerated kinds, start/stop times, and xaction-specific statistics. For more information, please refer to this [document](/xaction/README.md).

## Start xaction

`ais start <XACTION_COMMAND> [arguments...]`

Start xaction(s). Some xactions require additional arguments such as bucket name to execute.


### Examples

#### Start cluster-wide LRU

Starts LRU xaction on all nodes

```console
$ ais start lru
Started "lru" xaction.
```
An administrator may choose to run LRU on a subset of buckets. This can be achieved by using the `--buckets` flag to provide a comma-separated list of buckets, for instance `--buckets bck1,gcp://bck2`, on which LRU needs to be performed.
Additionally, the `--force`(`-f`) option can be used to override the bucket's `lru.enabled` property.

**Note:** To ensure safety, the force flag (`-f`) only works when a list of buckets is provided.
```console
$ ais start lru --buckets ais://buck1,aws://buck2 -f
```

## Stop xaction

`ais stop xaction XACTION_ID|XACTION_NAME [BUCKET_NAME]`

Stop xaction(s).
The second argument is used to determine the bucket name if it is required.

### Examples

#### Stop cluster-wide LRU

Stops currently running LRU xaction.

```console
$ ais stop xaction lru
Stopped "lru" xaction.
```

## Show xaction stats

`ais show xaction [XACTION_ID|XACTION_NAME] [BUCKET_NAME]`

Display details about `XACTION_ID` or `XACTION_NAME` xaction. If no arguments are given, displays details about all xactions.
The second argument is used to determine the bucket name if it is required.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output details in JSON format | `false` |
| `--all` | `bool` | If set, additionally displays old, finished xactions | `false` |
| `--active` | `bool` | If set, displays only running xactions | `false` |
| `--verbose` `-v` | `bool` | If set, displays extended information about xactions where available | `false` |

Certain extended actions have additional CLI. In particular, rebalance stats can also be displayed using the following command:

`ais show rebalance`

Display details about the last rebalane xaction.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh [N]` | `string` | watch the rebalance until it finishes or CTRL-C is pressed. Display the current stats every N seconds, where N ends with time suffix: s, m. If N is not defined it prints stats every 1 second | `1s` |
| `--all` | `bool` | If set, show all rebalance xactions | `false` |

Output of this command differs from the generic xaction output.

## Wait for xaction

`ais wait xaction XACTION_ID|XACTION_NAME [BUCKET_NAME]`

Wait for the `XACTION_ID` or `XACTION_NAME` xaction to finish.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh rate | `1s` |
