# Introduction, background, definitions

Batch operations that run asynchronously and may take seconds (minutes, hours, etc.) to execute - are called eXtended actions (xactions).

Internally, `xaction` is an abstraction at the root of the inheritance hierarchy that also contains specific user-visible jobs: `copy-bucket`, `evict-objects`, and more.

> For the most recently updated list of all supported jobs and their respective compile-time properties, see the [source](https://github.com/NVIDIA/aistore/blob/main/xact/api.go#L108).

**All jobs run asynchronously, have start and stop times, and common generic statistics**

Further, each and every job kind has its own display name, access permissions, scope (bucket and/or global), and a number of boolean properties - examples including:

| Property | Description |
| --- | --- |
| `Startable` | true if user can start this job via generic jobi-start API |
| `RefreshCap` | the system must refresh capacity stats upon the job's completion |

Many kinds of jobs can be manually started via generic job API (which's in turn utilized by the `ais start` command - see next).

Notable exceptions include electing new primary and listing objects in a given bucket - in both of those cases, there's a separate, more convenient and intuitive API that does the job, so to speak.

> Job starting, stopping (i.e., aborting), and monitoring commands all have equivalent *shorter* versions. For instance `ais start download` can be expressed as `ais start download`, while `ais wait copy-bucket Z8WkHxwIrr` is the same as `ais wait Z8WkHxwIrr`.

Rest of this document covers starting, stopping, and otherwise managing job kinds and specific job instances. For [job monitoring](/docs/cli/show.md#ais-show-job), please use `ais show job` command and its numerous subcommands and options.

* [`ais show job`](/docs/cli/show.md#ais-show-job)

### See also

- [static descriptors (source code)](https://github.com/NVIDIA/aistore/blob/main/xact/api.go#L108)
- [`xact` package README](https://github.com/NVIDIA/aistore/blob/main/xact/README.md).
- [`batch jobs`](/docs/batch.md)
- [CLI: `dsort` (distributed shuffle)](/docs/cli/dsort.md)
- [CLI: `download` from any remote source](/docs/cli/download.md)
- [built-in `rebalance`](/docs/rebalance.md)

* And more:
  - [`ais etl`](/docs/cli/etl.md)
  - [`ais prefetch`](/docs/cli/object.md)
  - [`ais cp`](#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)
  - [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges-and-entire-buckets)
  - [reading, writing, and listing archives](/docs/cli/object.md)
  - [copying buckets](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)

# `ais job` command

Has the following static completions aka subcommands:

```console
$ ais job <TAB-TAB>
start   stop    wait    rm     show

```
and further:

```console
$ ais job --help
NAME:
   ais job - monitor, query, start/stop and manage jobs and eXtended actions (xactions)

USAGE:
   ais job command [arguments...] [command options]

COMMANDS:
   start  run batch job
   stop   terminate a single batch job or multiple jobs (press <TAB-TAB> to select, '--help' for options)
   wait   wait for a specific batch job to complete (press <TAB-TAB> to select, '--help' for options)
   rm     cleanup finished jobs
   show   show running and finished jobs ('--all' for all, or press <TAB-TAB> to select, '--help' for options)

OPTIONS:
   --help, -h  show help
```

Notice, though, that `start`, stop`, and `wait` (verbs) have shorter versions, e.g.:

* `ais start` is a built-in alias for `ais job start`, and so on.

> For all configured pre-built and user-defined aliases (aka "shortcuts"), run `ais alias` or `ais alias --help`

## Table of Contents
- [Start job](#start-job)
- [Stop job](#stop-job)
- [Show job](#show-job)
  - [Show extended statistics](#show-extended-statistics)
- [Wait for job](#wait-for-job)
- [Distributed Sort](#distributed-sort)
- [Downloader](#downloader)

## Start job

`ais start <JOB_NAME> [arguments...]`

Start a certain job. Some jobs require additional arguments such as bucket name to execute.

Note: `job start download|dsort` have slightly different options. Please see their documentation for more:
* [`job start download`](download.md#start-download-job)
* [`job start dsort`](dsort.md#start-dsort-job)

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

## Stop job

Stop a single job or multiple jobs.

```console
$ ais stop --help
NAME:
   ais stop - (alias for "job stop") terminate a single batch job or multiple jobs, e.g.:
     - 'stop tco-cysbohAGL'       - terminate a given (multi-object copy/transform) job identified by its unique ID;
     - 'stop copy-listrange'      - terminate all multi-object copies;
     - 'stop copy-objects'        - same as above (using display name);
     - 'stop list'                - stop all list-objects jobs;
     - 'stop ls'                  - same as above;
     - 'stop prefetch-listrange'  - stop all prefetch jobs;
     - 'stop prefetch'            - same as above;
     - 'stop g731 --force'        - forcefully abort global rebalance g731 (advanced usage only);
     - 'stop --all'               - terminate all running jobs
   press <TAB-TAB> to select, '--help' for more options.

USAGE:
   ais stop [NAME] [JOB_ID] [NODE_ID] [BUCKET] [command options]

OPTIONS:
   --all          all running jobs
   --regex value  regular expression to select jobs by name, kind, or description, e.g.: --regex "ec|mirror|elect"
   --force, -f    force execution of the command (caution: advanced usage only)
   --yes, -y      assume 'yes' to all questions
   --help, -h     show help
```

### Examples stopping a single job:

* `ais stop download JOB_ID`
* `ais stop JOB_ID`
* `ais stop dsort JOB_ID`

### Examples stopping multiple jobs:

* `ais stop download --all`              # stop all downloads
* `ais stop copy-bucket ais://abc --all` # stop all `copy-bucket` jobs where the destination bucket is ais://abc
* `ais stop resilver t[rt2erGhbr]`       # ask target  t[rt2erGhbr] to stop resilvering

and more.

Note: `job stop download|dsort` have slightly different options. Please see their documentation for more:
* [`job stop download`](download.md#stop-download-job)
* [`job stop dsort`](dsort.md#stop-dsort-job)

### More Examples

#### Stop cluster-wide LRU

Stops currently running LRU eviction.

```console
$ ais stop lru
Stopped LRU eviction.
```

## Show job

`ais show job [NAME] [JOB_ID] [NODE_ID] [BUCKET] [command options]`

```console
$ ais show job --help
NAME:
   ais show job - Show running and/or finished jobs:
     archive        blob-download  cleanup     copy-bucket       copy-objects   delete-objects
     download       dsort          ec-bucket   ec-get            ec-put         ec-resp
     elect-primary  etl-bucket     etl-inline  etl-objects       evict-objects  evict-remote-bucket
     list           lru-eviction   mirror      prefetch-objects  promote-files  put-copies
     rebalance      rename-bucket  resilver    summary           warm-up-metadata
   (use any of these names with 'ais show job' command, or try shortcuts: "evict", "prefetch", "copy", "delete", "ec")
   e.g.:
     - show job prefetch-listrange         - show all running prefetch jobs;
     - show job prefetch                   - same as above;
     - show job prefetch --top 5           - show 5 most recent prefetch jobs;
     - show job tco-cysbohAGL              - show a given (multi-object copy/transform) job identified by its unique ID;
     - show job copy-listrange             - show all running multi-object copies;
     - show job copy-objects               - same as above (using display name);
     - show job copy                       - show all copying jobs including both bucket-to-bucket and multi-object;
     - show job copy-objects --all         - show both running and already finished (or stopped) multi-object copies;
     - show job copy-objects --all --top 10 - show 10 most recent multi-object copy jobs;
     - show job ec                         - show all erasure-coding;
     - show job list                       - show all running list-objects jobs;
     - show job ls                         - same as above;
     - show job ls --refresh 10            - same as above with periodic _refreshing_ every 10 seconds;
     - show job ls --refresh 10 --count 4  - same as above but only for the first four 10-seconds intervals;
     - show job prefetch --refresh 1m      - show all running prefetch jobs at 1 minute intervals (until Ctrl-C);
     - show job evict                      - all running bucket and/or data evicting jobs;
     - show job --all                      - show absolutely all jobs, running and finished.

USAGE:
   ais show job [NAME] [JOB_ID] [NODE_ID] [BUCKET] [command options]

OPTIONS:
   --all             Include all jobs: running, finished, and aborted
   --count value     Used together with '--refresh' to limit the number of generated reports, e.g.:
                      '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --date-time       Override the default hh:mm:ss (hours, minutes, seconds) time format - include calendar date as well
   --json, -j        JSON input/output
   --log value       Filename to log metrics (statistics)
   --no-headers, -H  Display tables without headers
   --progress        Show progress bar(s) and progress of execution in real time
   --refresh value   Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --regex value     Regular expression to select jobs by name, kind, or description, e.g.: --regex "ec|mirror|elect"
   --top value       Show top N most recent jobs (e.g., --top 5 to show the 5 most recent jobs)
   --units value     Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --verbose, -v     Show extended statistics
   --help, -h        Show help
```

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

### Examples using `--top` flag

Show 2 most recent list jobs:

```console
$ ais show job list --top 2
list[LIM_Ua6LV] (run options: ais://job-top-test, props:name,size,cached) 
NODE             ID              KIND    BUCKET                  OBJECTS         BYTES   START           END     STATE
ZXzt8081         LIM_Ua6LV       list    ais://job-top-test      6               36B     22:29:39        -       Running
list[q9MIgaNLO] (run options: ais://job-top-test, props:name,size,cached) 
NODE             ID              KIND    BUCKET                  OBJECTS         BYTES   START           END     STATE
ZXzt8081         q9MIgaNLO       list    ais://job-top-test      6               36B     22:29:39        -       Running
```

Show 1 most recent job of any type:

```console
$ ais show job --top 1
list[LIM_Ua6LV] (run options: ais://job-top-test, props:name,size,cached) 
NODE             ID              KIND    BUCKET                  OBJECTS         BYTES   START           END     STATE
ZXzt8081         LIM_Ua6LV       list    ais://job-top-test      6               36B     22:29:39        -       Running
```

Show 2 most recent list jobs including finished ones:

```console
$ ais show job list --all --top 2
list[LIM_Ua6LV] (run options: ais://job-top-test, props:name,size,cached) 
NODE             ID              KIND    BUCKET                  OBJECTS         BYTES   START           END     STATE
ZXzt8081         LIM_Ua6LV       list    ais://job-top-test      6               36B     22:29:39        -       Running
list[q9MIgaNLO] (run options: ais://job-top-test, props:name,size,cached) 
NODE             ID              KIND    BUCKET                  OBJECTS         BYTES   START           END     STATE
ZXzt8081         q9MIgaNLO       list    ais://job-top-test      6               36B     22:29:39        -       Running
```

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

`ais wait [NAME] [JOB_ID] [NODE_ID] [BUCKET]`

Wait for the specified job to finish.

> `job wait download|dsort` have slightly different options. Please see their documentation for more:
* [`job wait download`](download.md#wait-for-download-job)
* [`job wait dsort`](dsort.md#wait-for-dsort-job)

### Options

```console
$ ais wait --help

NAME:
   ais wait - (alias for "job wait") wait for a specific batch job to complete (press <TAB-TAB> to select, '--help' for more options)

USAGE:
   ais wait [NAME] [JOB_ID] [NODE_ID] [BUCKET] [command options]

OPTIONS:
   --progress       Show progress bar(s) and progress of execution in real time
   --refresh value  Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --timeout value  Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --help, -h       Show help
```

## Distributed Sort

`ais start dsort`

Run [dSort](/docs/dsort.md).
[Further reference for this command can be found here.](dsort.md)

## Downloader

`ais start download` or `ais start download`

Run the AIS [Downloader](/docs/README.md).
[Further reference for this command can be found here.](/docs/downloader.md)
