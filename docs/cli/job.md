---
layout: post
title: JOB
permalink: ./cli/job
redirect_from:
 - ./cli/job.md/
 - /docs/./cli/job.md/
---

# CLI Reference for Job (xaction) management

Batch operations that run asynchronously and may take many seconds (minutes, or even hours) to execute are called eXtended actions or xactions.
> Note: In CLI docs, the terms "xaction" and "job" are used interchangeably.

AIS *xactions* run asynchronously, have one of the enumerated kinds, start/stop times, and xaction-specific statistics. For more information, please refer to this [document](/xaction/README.md).

## Table of Contents
- [Start xaction](#start-xaction)
- [Stop xaction](#stop-xaction)
- [Show job statistics](#show-job-statistics)
	- [Show Job Extended Statistics](#show-job-extended-statistics)
- [Wait for xaction](#wait-for-xaction)
- [Distributed Sort](#distributed-sort)
- [Downloader](#downloader)

## Start Jobs

`ais job start <JOB_NAME> [arguments...]`

Start a certain job. Some jobs require additional arguments such as bucket name to execute.

Note: `job start download|dsort` have slightly different options. Please see their documentation for more:
* [`job start download`](download.md#start-download-job)
* [`job start dsort`](dsort.md#start-dsort-job)

### Examples

#### Start cluster-wide LRU

Starts LRU xaction on all nodes

```console
$ ais job start lru
Started "lru" xaction.
```
An administrator may choose to run LRU on a subset of buckets. This can be achieved by using the `--buckets` flag to provide a comma-separated list of buckets, for instance `--buckets bck1,gcp://bck2`, on which LRU needs to be performed.
Additionally, the `--force`(`-f`) option can be used to override the bucket's `lru.enabled` property.

**Note:** To ensure safety, the force flag (`-f`) only works when a list of buckets is provided.
```console
$ ais job start lru --buckets ais://buck1,aws://buck2 -f
```

## Stop Jobs

`ais job stop xaction XACTION_ID|XACTION_NAME [BUCKET]`

Stop a job. The bucket argument is used to determine the bucket name if it is required.

`ais job stop download JOB_ID`
`ais job stop dsort JOB_ID`

Note: `job stop download|dsort` have slightly different options. Please see their documentation for more:
* [`job stop download`](download.md#stop-download-job)
* [`job stop dsort`](dsort.md#stop-dsort-job)

### Examples

#### Stop cluster-wide LRU

Stops currently running LRU xaction.

```console
$ ais job stop xaction lru
Stopped "lru" xaction.
```

## Show Job Statistics

`ais show job xaction [XACTION_ID|XACTION_NAME] [BUCKET]`

Display details about `XACTION_ID` or `XACTION_NAME` xaction. If no arguments are given, displays details about all xactions.
The second argument is used to determine the bucket name if it is required.

Note: `job show download|dsort` have slightly different options. Please see their documentation for more:
* [`job show download`](download.md#show-download-jobs-and-job-status)
* [`job show dsort`](dsort.md#show-dsort-jobs-and-job-status)

### Show Job Extended Statistics

All jobs show the number of processed objects(column `OBJECTS`) and the total size of the data(column `BYTES`).
Both values are cumulative for the entire xaction life-time.
Some jobs provide extended statistics and it is available when you show information about a certain xaction name.

#### Show EC Encoding Statistics

The output contains a few extra columns:

- `ERRORS` - the total number of objects EC failed to encode
- `QUEUE` - the average length of working queue: the average number of objects waiting in the queue when a new EC encode request received. Values close to `0` mean that every object was processed immediately after the request had been received
- `AVG TIME` - the average total processing time for an object: from the moment the object is put to the working queue and to the moment the last encoded slice is sent to another target
- `ENC TIME` - the average amount of time spent on encoding an object.

The extended statistics may give a hint what is the possible bottleneck:

- high values in `QUEUE` - EC is congested and does not have time to process all incoming requests
- low values in `QUEUE` and `ENC TIME`, but high ones in `AVG TIME` may mean that the network is slow and a lot of time spent on sending the encoded slices
- low values in `QUEUE`, and `ENC TIME` close to `AVG TIME` may mean that the local hardware is overloaded: either local drives or CPUs are overloaded.

#### Show EC Restoring Statistics
Show information about EC restore requests.

The output contains a few extra columns:

- `ERRORS` - the total number of objects EC failed to restore
- `QUEUE` - the average length of working queue: the average number of objects waiting in the queue when a new EC encode request received. Values close to `0` mean that every object was processed immediately after the request had been received
- `AVG TIME` - the average total processing time for an object: from the moment the object is put to the working queue and to the moment the last encoded slice is sent to another target

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output details in JSON format | `false` |
| `--all` | `bool` | If set, additionally displays old, finished xactions | `false` |
| `--active` | `bool` | If set, displays only running xactions | `false` |
| `--verbose` `-v` | `bool` | If set, displays extended information about xactions where available | `false` |

Certain extended actions have additional CLI. In particular, rebalance stats can also be displayed using the following command:

`ais show rebalance`

Display details about the most recent rebalance xaction.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh [N]` | `string` | watch the rebalance until it finishes or CTRL-C is pressed. Display the current stats every N seconds, where N ends with time suffix: s, m. If N is not defined it prints stats every 1 second | `1s` |
| `--all` | `bool` | If set, show all rebalance xactions | `false` |

Output of this command differs from the generic xaction output.

## Wait for Jobs

`ais job wait xaction XACTION_ID|XACTION_NAME [BUCKET]`

Wait for the `XACTION_ID` or `XACTION_NAME` xaction to finish.

Note: `job wait download|dsort` have slightly different options. Please see their documentation for more:
* [`job wait download`](download.md#wait-for-download-job)
* [`job wait dsort`](dsort.md#wait-for-dsort-job)

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh rate | `1s` |

## Distributed Sort

`ais job start dsort`

Run [dSort](/docs/dsort.md).
[Further reference for this command can be found here.](dsort.md)

## Downloader

`ais job start download`

Run the AIS [Downloader](/docs/README.md).
[Further reference for this command can be found here.](downloader.md)
