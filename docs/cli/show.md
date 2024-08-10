---
layout: post
title: SHOW
permalink: /docs/cli/show
redirect_from:
 - /cli/show.md/
 - /docs/cli/show.md/
---

# `ais show` command

AIS CLI `show` command can universally be used to view summaries and details on a cluster and its nodes, buckets and objects, running and finished jobs - in short, _all_ managed entities (see below). The command is a "hub" for all information-viewing commands that are currently supported.

For easy usage, all `show` commands have been aliased to their respective top-level counterparts:

```console
$ ais show <command>
```
is equivalent to:
```console
$ ais <command> show
```

> For instance, `ais show performance` is an alias for `ais performance` - both can be used interchangeably.

> As a general rule, instead of remembering any of the above (as well as any of the below), type (e.g.) `ais perf<TAB-TAB>` and press `Enter`.

> You can also hit <TAB-TAB>` maybe a few more times while typing a few more letters in between. Any combination will work.

> When part-typing, part-TABing a sequence of words that (will eventually) constitute a valid AIS CLI command, type `--help` at any time. This will display short description, command-line options, usage examples, and other context-sensitive information.

> For usage examples, another way to quickly find them would be a good-and-old keyword search. E.g., `cd aistore; grep -n "ais.*cp" docs/cli/*.md`, and similar. Additional useful tricks (including `ais search`) are also mentioned elsewhere in the documentation.

As far as `ais show`, the command currently extends as follows:

```console
$ ais show <TAB-TAB>
auth             bucket           performance      rebalance        remote-cluster   log
object           cluster          storage          config           job
```

In other words, there are currently 11 subcommands that are briefly described in the rest of this text.

## Table of Contents
- [`ais show performance`](#ais-show-performance)
- [`ais show job`](#ais-show-job)
- [`ais show cluster`](#ais-show-cluster)
- [`ais show auth`](#ais-show-auth)
- [`ais show bucket`](#ais-show-bucket)
- [`ais show object`](#ais-show-object)
- [`ais show storage`](#ais-show-storage)
- [`ais show config`](#ais-show-config)
- [`ais show remote-cluster`](#ais-show-remote-cluster)
- [`ais show rebalance`](#ais-show-rebalance)
- [`ais show log`](#ais-show-log)

## `ais show performance`

The command has 5 subcommands:

```console
$ ais show performance
counters     throughput   latency      capacity     disk
```

The command provides a quick single-shot overview of cluster performance. If you type `ais show performance` and instead of `<TAB-TAB>` hit `Enter`, the command will show performance counters, aggregated cluster throughput statistics, and the other 3 tables (latency, capacity, disk IO) - in sequence, one after another.

The command's help screen follows below - notice the command-line options (aka flags):

```console
$ ais show performance --help
NAME:
   ais show performance - show performance counters, throughput, latency, disks, used/available capacities (press <TAB-TAB> to select specific view)

USAGE:
   ais show performance command [command options] [TARGET_ID]

COMMANDS:
   counters    show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
               - numbers of list-objects requests;
               - (GET, PUT, etc.) cumulative and average sizes;
               - associated error counters, if any.
   throughput  show GET and PUT throughput, associated (cumulative, average) sizes and counters
   latency     show GET, PUT, and APPEND latencies and average sizes
   capacity    show target mountpaths, disks, and used/available capacity
   disk        show disk utilization and read/write statistics

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --all             when printing tables, show all columns including those that have only zero values
   --no-headers, -H  display tables without headers
   --regex value     regular expression to select table columns (case-insensitive), e.g.: --regex "put|err"
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --average-size    show average GET, PUT, etc. request size
   --help, -h        show help
```

As one would maybe expect, `--refresh`, `--units` and all the other listed flags universally work across all `performance` subcommands.

For instance, you could continuously run several screens to simultaneously display a variety of performance-related aspects:

```console
# screen #1

$ ais show performance counters --refresh 5
```

```console
# screen #2

$ ais show performance throughput --refresh 5
```

and so on - all the 5 tables with 5-seconds periodicity, or any other time interval(s) of your choosing.

As far as continuous monitoring goes, this (approach) has a chance to provide a good overall insight. A poor-man's addition, if you will, to the popular (and also supported) tools such as Grafana and Prometheus. But available with zero setup out of the box (which is a plus).

### What's running

Use `ais show performance` and its variations in combination with `ais show job` (and variations). The latter shows what's running in the cluster, and thus combining the two may make sense.

### See also

* [Observability](/docs/metrics.md)
* [Prometheus](/docs/prometheus.md)

## `ais show job`

The command has no statically defined subcommands. When you type `ais show job <TAB-TAB>`, the resulting set of shell completions will only include job names (aka "kinds") that are **currently running**. Example:

```console
$ ais show job <TAB-TAB>
move-bck    rebalance
```

Just maybe to reiterate the same slightly differently: `ais show job <TAB-TAB>` won't produce anything useful iff the cluster currently doesn't run any jobs

On the other hand, job-identifying selections `[NAME] [JOB_ID] [NODE_ID] [BUCKET]`:

1. are all optional, and
2. can be typed in an arbitrary order

Example:

```console
$ ais show job xhcTyUfJF t[Kritxhbt]
```

is the same as:

```console
$ ais show job t[Kritxhbt] xhcTyUfJF
```

In both cases, we are asking a specific target node (denoted as `NODE_ID` in the command's help) for a specific job ID (denoted as `JOB_ID`).

Selection-wise, this would be the case of ultimate specificity. Accordingly, on the opposite side of the spectrum would be something like:

```console
$ ais show job --all
```

The `--all` flag always defines the broadest scope possible, and so the query `ais show job --all` includes absolutely all jobs, running and finished (succeeded and aborted).

Here's at a glance:

```console
$ ais show job --help
NAME:
   ais show job - show running and finished jobs ('--all' for all, or press <TAB-TAB> to select, '--help' for options)

USAGE:
   ais show job [command options] [NAME] [JOB_ID] [NODE_ID] [BUCKET]

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --json, -j        json input/output
   --all             all jobs, including finished and aborted
   --regex value     regular expression to select jobs by name, kind, or description, e.g.: --regex "ec|mirror|elect"
   --no-headers, -H  display tables without headers
   --verbose, -v     verbose
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --progress        show progress bar(s) and progress of execution in real time
   --log value       path to file where the metrics will be saved
   --help, -h        show help
```

### Example: show all currently running jobs, and narrow the selection to a given target node:

```console

$ ais show job t[ugoFtqUrrm]
NODE          ID            KIND         BUCKET                     OBJECTS     BYTES        START           END             STATE
ugoFtqUrrm    vOYSo5pHG     ec-get       mybucket-ec-rebalance      -           -            12-03 10:32:25  -               Running
ugoFtqUrrm    b4Ks45pHv     ec-get       mybucket-obj-n-slice       9           42.36MiB     12-03 10:31:33  -               Running
ugoFtqUrrm    vUYSo5pHvS    ec-put       mybucket-ec-rebalance      3           1.43MiB      12-03 10:32:25  -               Running
ugoFtqUrrm    Kobs45pHvS    ec-put       mybucket-obj-n-slice       9           4.75MiB      12-03 10:31:33  -               Running
ugoFtqUrrm    U8UcSo5pHv    ec-resp      mybucket-ec-rebalance      18          89.45MiB     12-03 10:32:25  -               Running
ugoFtqUrrm    M8M6sodqmv    ec-resp      mybucket-obj-n-slice       13          64.49MiB     12-03 10:31:34  -               Idle
ugoFtqUrrm    Ioa31VqaB     list         mybucket-ec-rebalance      5           -            12-03 10:32:32  12-03 10:32:42  Aborted
ugoFtqUrrm    X3H381Vqau    list         mybucket-ec-rebalance      3           -            12-03 10:32:29  12-03 10:32:42  Aborted
ugoFtqUrrm    g5            rebalance    -                          6           25.59MiB     12-03 10:32:32  12-03 10:32:39  Finished
```

### Example: show a job by name, and include finished/aborted:

```console
$ ais show job resilver --all
resilver[G0p7yXYiUg]
NODE             ID              KIND            OBJECTS         BYTES           START           END             STATE
HAAt8090         G0p7yXYiUg      resilver        11              18.38KiB        13:04:51        13:04:51        Finished
JVnt8086         G0p7yXYiUg      resilver        10              14.86KiB        13:04:51        13:04:51        Finished
LDgt8088         G0p7yXYiUg      resilver        14              20.81KiB        13:04:51        13:04:51        Finished
OBIt8089         G0p7yXYiUg      resilver        4               7.04KiB         13:04:51        13:04:51        Finished
VUCt8091         G0p7yXYiUg      resilver        9               14.50KiB        13:04:51        13:04:51        Finished
qVJt8087         G0p7yXYiUg      resilver        9               14.21KiB        13:04:51        13:04:51        Finished
```

> Here and elsewhere in the documentation, CLI colors used to highlight certain (notable) items on screen - are not shown.

> On a related note: coloring can be disabled via `ais config cli set no_color`.

### Example: continuous monitoring

```console
$ ais show job --refresh 5

rebalance[g2]
NODE             ID      KIND            OBJECTS         BYTES           START           END     STATE
HAAt8090         g2      rebalance       2072            3.05MiB         13:35:00        -       Running
JVnt8086         g2      rebalance       2               2.08KiB         13:35:00        -       Running
LDgt8088         g2      rebalance       4               6.46KiB         13:35:00        -       Running
OBIt8089         g2      rebalance       4               7.39KiB         13:35:00        -       Running
VUCt8091         g2      rebalance       -               -               13:35:00        -       Running
qVJt8087         g2      rebalance       -               -               13:35:00        -       Running
------------------------------------------------------------------------
rebalance[g2]
NODE             ID      KIND            OBJECTS         BYTES           START           END     STATE
HAAt8090         g2      rebalance       2072            3.05MiB         13:35:00        -       Running
JVnt8086         g2      rebalance       2               2.08KiB         13:35:00        -       Running
LDgt8088         g2      rebalance       4               6.46KiB         13:35:00        -       Running
OBIt8089         g2      rebalance       4               7.39KiB         13:35:00        -       Running
VUCt8091         g2      rebalance       -               -               13:35:00        -       Running
qVJt8087         g2      rebalance       -               -               13:35:00        -       Running
------------------------------------------------------------------------
No running jobs. Use '--all' to show all, '--all' <TAB-TAB> to select, '--help' for details.
------------------------------------------------------------------------
ec-get[wHUgmjFaJ]
NODE             ID              BUCKET                                  OBJECTS         BYTES   ERRORS  QUEUE   AVG TIME        START           END     ABORTED
OBIt8089         wHUgmjFaJ       ais://TESTAISBUCKET-ec-rebalance        -               -       -       -       -               13:35:24        -       false
ec-get[P3SqrpURnv]
NODE             ID              BUCKET                                  OBJECTS         BYTES   ERRORS  QUEUE   AVG TIME        START           END     ABORTED
VUCt8091         P3SqrpURnv      ais://TESTAISBUCKET-ec-rebalance        -               -       -       -       -               13:35:24        -       false
ec-get[u3pGCzmh4]
NODE             ID              BUCKET                                  OBJECTS         BYTES   ERRORS  QUEUE   AVG TIME        START           END     ABORTED
JVnt8086         u3pGCzmh4       ais://TESTAISBUCKET-ec-rebalance        -               -       -       -       -               13:35:24        -       false

...

list[n2O4CJxUg]
NODE             ID              KIND    BUCKET                                  OBJECTS         BYTES           START           END     STATE
HAAt8090         n2O4CJxUg       list    ais://TESTAISBUCKET-ec-rebalance        10              32.51MiB        13:36:55        -       Idle
JVnt8086         n2O4CJxUg       list    ais://TESTAISBUCKET-ec-rebalance        16              52.96MiB        13:36:55        -       Idle
LDgt8088         n2O4CJxUg       list    ais://TESTAISBUCKET-ec-rebalance        2               3.59MiB         13:36:55        -       Idle
OBIt8089         n2O4CJxUg       list    ais://TESTAISBUCKET-ec-rebalance        6               11.72MiB        13:36:55        -       Idle
VUCt8091         n2O4CJxUg       list    ais://TESTAISBUCKET-ec-rebalance        10              37.53MiB        13:36:55        -       Idle
list[J-bfCJxVp]
NODE             ID              KIND    BUCKET                                  OBJECTS         BYTES           START           END     STATE
qVJt8087         J-bfCJxVp       list    ais://TESTAISBUCKET-ec-rebalance        22              60.91MiB        13:37:18        -       Idle
rebalance[g8]
NODE             ID      KIND            OBJECTS         BYTES           START           END     STATE
HAAt8090         g8      rebalance       4               27.46MiB        13:37:18        -       Running
JVnt8086         g8      rebalance       2               4.42MiB         13:37:18        -       Running
LDgt8088         g8      rebalance       2               9.96MiB         13:37:18        -       Running
OBIt8089         g8      rebalance       4               23.49MiB        13:37:18        -       Running
VUCt8091         g8      rebalance       32              -               13:37:18        -       Running
qVJt8087         g8      rebalance       6               39.38MiB        13:37:18        -       Running
^[^C$ ais show job --refresh 5
rebalance[g15]
NODE             ID      KIND            OBJECTS         BYTES           START           END     STATE
HAAt8090         g15     rebalance       648             971.49KiB       13:40:54        -       Running
JVnt8086         g15     rebalance       688             1.01MiB         13:40:54        -       Running
LDgt8088         g15     rebalance       554             833.21KiB       13:40:54        -       Running
OBIt8089         g15     rebalance       734             1.07MiB         13:40:54        -       Running
VUCt8091         g15     rebalance       -               -               13:40:54        -       Running
qVJt8087         g15     rebalance       694             1.02MiB         13:40:54        -       Running
------------------------------------------------------------------------

...
```

### See also

- [definitions: `xaction` vs `job`](/docs/batch.md)
- [CLI: `ais job` command](/docs/cli/job.md)
- [CLI: `dsort` (distributed shuffle)](/docs/cli/dsort.md)
- [CLI: `download` from any remote source](/docs/cli/download.md)
- [built-in `rebalance`](/docs/rebalance.md)
- [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges)
- [reading, writing, and listing archives](/docs/cli/object.md)
- [copying buckets](/docs/cli/bucket.md#copy-bucket)

## `ais show cluster`

The first command to think of when deploying a new cluster. Useful as well when looking for the shortest quickest summary of what's running and what's going on. The subcommands and brief description follows:

```console
# ais show cluster <TAB-TAB>
proxy    target   smap     bmd      config   stats
```

```console
$ ais show cluster --help
NAME:
   ais show cluster - main dashboard: show cluster at-a-glance (nodes, software versions, utilization, capacity, memory and more)

USAGE:
   ais show cluster command [command options] [NODE_ID] | [target [NODE_ID]] | [proxy [NODE_ID]] | [smap [NODE_ID]] | [bmd [NODE_ID]] | [config [NODE_ID]] | [stats [NODE_ID]]

COMMANDS:
   smap    show cluster map (Smap)
   bmd     show bucket metadata (BMD)
   config  show cluster and node configuration
   stats   (alias for "ais show performance") show performance counters, throughput, latency, disks, used/available capacities (press <TAB-TAB> to select specific view)

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --json, -j        json input/output
   --no-headers, -H  display tables without headers
   --help, -h        show help
```

### Example usage with no parameters and a _different_ endpoint

```console
$ export AIS_ENDPOINT=http://10.0.1.148:51080
$ ais show cluster

PROXY            MEM USED(%)     MEM AVAIL       UPTIME  K8s POD         STATUS
p[EciZrNdH][P]   0.01%           363.51GiB       94d     ais-proxy-2     online
p[HPpnlgpj]      0.01%           363.66GiB       94d     ais-proxy-0     online
p[LZOkYuAf]      0.01%           362.62GiB       94d     ais-proxy-8     online
p[NXDmWuAV]      0.01%           363.68GiB       94d     ais-proxy-6     online
p[OzRhyuOB]      0.01%           363.73GiB       94d     ais-proxy-4     online
p[WwpvNugq]      0.01%           363.17GiB       94d     ais-proxy-9     online
p[cAQpRMET]      0.01%           363.56GiB       94d     ais-proxy-1     online
p[eYQteCHG]      0.01%           363.81GiB       94d     ais-proxy-7     online
p[ehkGLcSD]      0.01%           363.57GiB       94d     ais-proxy-3     online
p[reZqfbjy]      0.01%           363.67GiB       94d     ais-proxy-5     online

TARGET           MEM USED(%)     MEM AVAIL       CAP USED(%)     CAP AVAIL       CPU USED(%)    UPTIME   K8s POD         STATUS
t[KopwySra]      0.15%           363.54GiB       77.67%          31.825TiB       15.38%         94d      ais-target-1    online
t[MgHbIvNG]      0.16%           363.65GiB       77.00%          30.941TiB       14.06%         94d      ais-target-4    online
t[WoLgoQEW]      0.16%           363.72GiB       78.00%          31.763TiB       17.67%         94d      ais-target-7    online
t[fXFQnenn]      0.11%           363.83GiB       78.00%          31.751TiB       18.12%         94d      ais-target-6    online
t[fwKlswQP]      0.16%           363.62GiB       78.33%          31.758TiB       19.81%         94d      ais-target-5    online
t[tFUiHCCO]      0.16%           363.76GiB       79.00%          31.617TiB       19.47%         94d      ais-target-8    online
t[tfNkAtFk]      0.16%           363.55GiB       77.33%          31.875TiB       19.70%         94d      ais-target-2    online
t[uxvpIDPc]      0.15%           362.57GiB       77.67%          31.846TiB       16.56%         94d      ais-target-0    online
t[vAWmZZPv]      0.11%           363.68GiB       78.67%          31.608TiB       23.09%         94d      ais-target-3    online
t[wSJzGVnU]      0.10%           363.33GiB       76.67%          30.972TiB       17.13%         94d      ais-target-9    online

Summary:
   Proxies:             10 (0 unelectable)
   Targets:             10
   Cluster Map:         version 1512, UUID AGetvIKTz, primary p[EciZrNdH]
   Deployment:          K8s
   Status:              20 online
   Rebalance:           n/a
   Authentication:      disabled
   Version:             3.12.85636aa
   Build:               2022-11-16T17:55:50+0000
```

> `AIS_ENDPOINT` is part of the `AIS_**` [environment](https://github.com/NVIDIA/aistore/blob/main/api/env/ais.go).

> `AIS_ENDPOINT` can point to any AIS gateway (proxy) in a given cluster. Does not necessarily have to be the _primary_ gateway.

> For CLI, in particular, `AIS_ENDPOINT` overrides cluster's endpoint that's currently configured. To view or change the configured endpoint (or any other CLI configuration item), run `ais config cli`.

### See also

* [`ais cluster` command](cluster.md#cluster-or-daemon-status)


## `ais show auth`
The following subcommands are currently supported:

```console
   cluster  show registered clusters
   role     show existing user roles
   user     show users or user details
   config   show AuthN server configuration
```

[Refer to `ais auth` documentation for details and examples.](auth.md#command-list)


## `ais show bucket`
Show bucket properties.

[Refer to `ais bucket` documentation for details and examples.](bucket.md#show-bucket-properties)

## `ais show object`
Show object details.

[Refer to `ais object` documentation for details and examples.](object.md#show-object-properties)

## `ais show storage`
Show storage usage and utilization in the cluster. Show disks and mountpaths - for a single selected node or for all storage nodes.

When run with no subcommands, `ais show storage` defaults to `ais show storage disk`.

In addition, the command support the following subcommands:

```console
# ais show storage <TAB-TAB>
disk        mountpath   capacity    summary
```

And with brief subcommand descriptions:

```console
$ ais show storage --help
NAME:
   ais show storage - show storage usage and utilization, disks and mountpaths

USAGE:
   ais show storage command [command options] [TARGET_ID]

COMMANDS:
   disk       show disk utilization and read/write statistics
   mountpath  show target mountpaths
   capacity   show target mountpaths, disks, and used/available capacity
   summary    show bucket sizes and %% of used capacity on a per-bucket basis

OPTIONS:
   --refresh value  interval for continuous monitoring;
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --count value    used together with '--refresh' to limit the number of generated reports (default: 0)
   --json, -j       json input/output
   --help, -h       show help
```

[Refer to `ais storage` documentation for details and examples.](storage.md)

## `ais show config`
Show daemon configuration.

[Refer to `ais cluster` documentation for details and examples.](config.md##show-configuration)

## `ais show remote-cluster`
Show information about attached AIS clusters.

[Refer to `ais cluster` documentation for details and examples.](cluster.md#show-remote-clusters)

## `ais show rebalance`

Display details about the most recent rebalance xaction.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh` | `duration` | Watch global rebalance at a given refresh interval. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds). Press Ctrl-C to stop monitoring. | ` ` |
| `--all` | `bool` | If set, show all rebalance xactions | `false` |

### Example

```console
$ ais show rebalance
REB ID   NODE        OBJECTS RECV   SIZE RECV   OBJECTS SENT   SIZE SENT   START TIME       END TIME   ABORTED
g1       CASGt8088   0              0B          0              0B          03-25 17:33:54   -          false
g1       DMwvt8089   0              0B          0              0B          03-25 17:33:54   -          false
g1       ejpCt8086   0              0B          0              0B          03-25 17:33:54   -          false
g1       kiuvt8091   0              0B          0              0B          03-25 17:33:54   -          false
g1       oGvbt8090   0              0B          0              0B          03-25 17:33:54   -          false
g1       xZntt8087   0              0B          0              0B          03-25 17:33:54   -          false

$ ais show rebalance
REB ID   NODE        OBJECTS RECV   SIZE RECV   OBJECTS SENT   SIZE SENT   START TIME       END TIME         ABORTED
g1       CASGt8088   0              0B          0              0B          03-25 17:33:54   03-25 17:34:09   false
g1       DMwvt8089   0              0B          0              0B          03-25 17:33:54   03-25 17:34:08   false
g1       ejpCt8086   0              0B          0              0B          03-25 17:33:54   03-25 17:34:08   false
g1       kiuvt8091   0              0B          0              0B          03-25 17:33:54   03-25 17:34:08   false
g1       oGvbt8090   0              0B          0              0B          03-25 17:33:54   03-25 17:34:08   false
g1       xZntt8087   0              0B          0              0B          03-25 17:33:54   03-25 17:34:09   false

Rebalance completed.
```

## `ais show log`

There are 3 enumerated log severities and, respectively, 3 types of logs generated by each node:
* error
* warning
* info

### Example 1. Show "info" log:

```console
# Use <TAB-TAB> auto-completion to select a node (run `ais show cluster` to show details)
$ ais show log
p[f6ytNhIhb]   p[OqlWpgwrY] ...
t[jkrt8Nkqi]   t[Juwzq371P] ...

# Type `p[O`<TAB-TAB> to complete the node ID and then use `less` (for instance) to search, scroll or page down (or up), etc.
$ ais show log p[OqlWpgwrY] | less

Log file created at: 2021/04/11 10:58:38
Running on machine: u18044
Binary: Built with gc go1.15.3 for linux/amd64
Log line format: L hh:mm:ss.uuuuuu file:line] msg
I 10:58:38.122973 config.go:1611 log.dir: "/ais/log"; l4.proto: tcp; port: 51080; verbosity: 3
...
:
```

### Example 2: show errors and/or warnings

By default, `ais show log` shows "info" log (that also contains all warnings and errors).

To show _only_ errors, run:
```console
ais show log OqlWpgwrY --severity=error

# or, same
ais show log OqlWpgwrY --severity=e | more
```

For warnings _and_ errors, run:
```console
ais show log OqlWpgwrY --severity=warning

# or, same
ais show log OqlWpgwrY --severity=w | less
```

