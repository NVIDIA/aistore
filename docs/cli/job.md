---
layout: post
title: JOB
permalink: /docs/cli/job
redirect_from:
 - /cli/job.md/
 - /docs/cli/job.md/
---

# CLI Reference for Job (xaction) management

Batch operations that run asynchronously and may take many seconds (minutes, or even hours) to execute are called eXtended actions or xactions.

All supported *jobs* run asynchronously, have one of the enumerated kinds, start/stop times, and common (generic) statistics.

> Job starting, stopping (i.e., aborting), and monitoring commands all have equivalent *shorter* versions. For instance `ais job start download` can be expressed as `ais start download`, while `ais job wait copy-bucket Z8WkHxwIrr` is the same as `ais wait Z8WkHxwIrr`.

> For additional information, please refer to this [readme](/xaction/README.md).

## Table of Contents
- [Start job](#start-job)
- [Stop job](#stop-job)
- [Show job statistics](#show-job-statistics)
  - [Show extended statistics](#show-extended-statistics)
- [Wait for job](#wait-for-job)
- [Distributed Sort](#distributed-sort)
- [Downloader](#downloader)

## Start job

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

## Stop job

`ais job stop [NAME] [JOB_ID] [NODE_ID] [BUCKET]`

Stop a single job or multiple jobs.

### Examples stopping a single job:

* `ais job stop download JOB_ID`
* `ais job stop JOB_ID`
* `ais job stop dsort JOB_ID`

### Examples stopping multiple jobs:

* `ais job stop download --all`              # stop all downloads
* `ais job stop copy-bucket ais://abc --all` # stop all `copy-bucket` jobs where the destination bucket is ais://abc
* `ais job stop resilver t[rt2erGhbr]`       # ask target  t[rt2erGhbr] to stop resilvering

and more.

Note: `job stop download|dsort` have slightly different options. Please see their documentation for more:
* [`job stop download`](download.md#stop-download-job)
* [`job stop dsort`](dsort.md#stop-dsort-job)

### More Examples

#### Stop cluster-wide LRU

Stops currently running LRU eviction.

```console
$ ais job stop lru
Stopped LRU eviction.
```

## Show job statistics

`ais show job [NAME] [JOB_ID] [NODE_ID] [BUCKET]`

You can show jobs by any combination of the optional (filtering) arguments: NAME, JOB_ID, etc..

Use `--all` option to include finished (or aborted) jobs.

As usual, press `<TAB-TAB> to select and see `--help` for details.

> `job show download|dsort` have slightly different options. Please see their documentation for more:
* [`job show download`](download.md#show-download-jobs-and-job-status)
* [`job show dsort`](dsort.md#show-dsort-jobs-and-job-status)

### Show extended statistics

All jobs show the number of processed objects(column `OBJECTS`) and the total size of the data(column `BYTES`).
Both values are cumulative for the entire job's life-time.

Certain kinds of supported jobs provide extended statistics, including:

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
| `--verbose` `-v` | `bool` | If set, displays all xaction statistics including extended ones. If the number of xaction to display is greater than one, the flag is ignored. | `false` |

Certain extended actions have additional CLI. In particular, rebalance stats can also be displayed using the following command:

`ais show rebalance`

Display details about the most recent rebalance xaction.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds). Ctrl-C to stop monitoring. | ` ` |
| `--all` | `bool` | If set, show all rebalance xactions | `false` |

Output of this command differs from the generic xaction output.

### Examples

Default compact tabular view:

```console
$ ais show job --all
NODE             ID              KIND    BUCKET                          OBJECTS         BYTES           START           END             STATE
zXZXt8084        FXjl0NWGOU      ec-put  TESTAISBUCKET-ec-mpaths         5               4.56MiB         12-02 13:04:50  12-02 13:04:50  Aborted
```

Verbose tabular view:

```console
$ ais show job FXjl0NWGOU --verbose
PROPERTY                 VALUE
.aborted                 true
.bck                     ais://TESTAISBUCKET-ec-mpaths
.end                     12-02 13:04:50
.id                      FXjl0NWGOU
.kind                    ec-put
.start                   12-02 13:04:50
ec.delete.err.n          0
ec.delete.n              0
ec.delete.time           0s
ec.encode.err.n          0
ec.encode.n              5
ec.encode.size           4.56MiB
ec.encode.time           16.964552ms
ec.obj.process.time      17.142239ms
ec.queue.len.n           0
in.obj.n                 0
in.obj.size              0
is_idle                  true
loc.obj.n                5
loc.obj.size             4.56MiB
out.obj.n                0
out.obj.size             0
```

## Wait for job

`ais job wait [NAME] [JOB_ID] [NODE_ID] [BUCKET]`

Wait for the specified job to finish.

> `job wait download|dsort` have slightly different options. Please see their documentation for more:
* [`job wait download`](download.md#wait-for-download-job)
* [`job wait dsort`](dsort.md#wait-for-dsort-job)

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds) | ` ` |

## Distributed Sort

`ais job start dsort` or `ais start dsort`

Run [dSort](/docs/dsort.md).
[Further reference for this command can be found here.](dsort.md)

## Downloader

`ais job start download` or `ais start download`

Run the AIS [Downloader](/docs/README.md).
[Further reference for this command can be found here.](downloader.md)
