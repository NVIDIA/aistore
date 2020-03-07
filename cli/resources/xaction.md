---
layout: post
title: XACTION
permalink: cli/resources/xaction
redirect_from:
- cli/resources/xaction.md/
---

## Extended Actions (X-actions or xactions)

The CLI allows users to interact with AIStore [Xactions](../../docs/xaction.md).

### Start

`ais start xaction XACTION_NAME [BUCKET_NAME]`

Start xaction(s). Some xactions require a bucket name to execute.
The second argument is used to determine the bucket name if it is required.

| Command | Description |
| --- | --- |
| `ais start xaction lru` | Starts cluster-wide LRU xaction |

### Stop

`ais stop xaction XACTION_NAME|all [BUCKET_NAME]`

Stop xaction(s). If the first argument is `all`, all xactions are stopped.
The second argument is used to determine the bucket name if it is required.


| Command | Description |
| --- | --- |
| `ais stop xaction rebalance` | Stops cluster rebalance. No effect if rebalance is not running |

### Show stats

`ais show xaction [XACTION_NAME] [BUCKET_NAME]`

Display details about `XACTION_NAME` xaction. If no arguments are given, displays details about all xactions.
The second argument is used to determine the bucket name if it is required.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output details in JSON format | `false` |
| `--all-items` | `bool` | If set, additionally displays old, finished xactions | `false` |
| `--active` | `bool` | If set, displays only running xactions | `false` |
| `--verbose` `-v` | `bool` | If set, displays extended information about xactions where available | `false` |

Certain extended actions have additional CLI. In particular, global rebalance stats can also be displayed using the following command:

`ais show rebalance`

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh [N]` | `string` | watch the global rebalance until it finishes or CTRL-C is pressed. Display the current stats every N seconds, where N ends with time suffix: s, m. If N is not defined it prints stats every 1 second | `1s` |

Output of this command differs from the generic xaction output.

### Wait

`ais wait xaction XACTION_NAME [BUCKET_NAME]`

Wait for the `XACTION_NAME` xaction to finish.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh rate | `1s` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais wait xaction lru` | Waits for the LRU to finish |
| `ais wait xaction copybck bck_name` | Waits for the `copybck` xaction that runs on `bck_name` to finish |
