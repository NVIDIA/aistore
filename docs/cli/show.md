---
layout: post
title: SHOW
permalink: /docs/cli/show
redirect_from:
 - /cli/show.md/
 - /docs/cli/show.md/
---

# CLI Reference for `show` commands

AIS CLI `show` command can universally be used to view summaries and details on a cluster and its nodes, buckets and objects, running and finished jobs - in short, _all_ managed entities (see below). The command is a "hub" for all information-viewing commands that are currently supported.

For easy usage, all `show` commands have been aliased to their respective top-level counterparts:

```console
$ ais show <command>

# is equivalent to:

$ ais <command> show
```

Running `ais show cluster` is the same as `ais cluster show`, and so on. And one more example of two identical commands:

```console
$ ais show performance
$ ais performance show
```

> As a general rule, instead remembering any of the above (and/or below), simply type (for instance) `ais perf<TAB-TAB>` and press `<TAB-TAB>` followed by `Enter`.

> When part-typing, part-TABing a sequence of words that constitute an AIS command, you can stop at any time and type `--help`. This will display short usage description, command-line options, examples, and other extended context.

> As far as usage examples, another way to quickly find them would be a good-and-old keyword search. E.g., `grep -n "ais.*cp" docs/cli/*.md`, and similar. Additional ways and useful tricks (including `ais search`) are also mentioned elsewhere in the docs.

As far as `ais show` itself, the command currently extends as follows:

```console
$ ais show <TAB-TAB>
auth             bucket           performance      rebalance        remote-cluster   log
object           cluster          storage          config           job

```

In other words, there are currently 11 subcommands that are briefly described in the rest of this text.

## Table of Contents
- [`ais show performance`](#ais-show-performance)
- [`ais show auth`](#ais-show-auth)
- [`ais show bucket`](#ais-show-bucket)
- [`ais show object`](#ais-show-object)
- [`ais show cluster`](#ais-show-cluster)
- [`ais show storage`](#ais-show-storage)
- [`ais show job`](#ais-show-job)
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
   ais show performance - show performance counters, throughput, latency, and more (press <TAB-TAB> to select specific view)

USAGE:
   ais show performance command [command options] [TARGET_ID]

COMMANDS:
   counters    show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
               - numbers of list-objects requests;
               - (GET, PUT, etc.) cumulative and average sizes;
               - associated error counters, if any, and more.
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

and so on - all with 5 seconds interval, or any other interval(s) of your choosing.

As far as continuous monitoring goes, this will provide a good overview. A poor-man's addition, if you will, to the (supported) tools such as Grafana and Prometheus, but available out of the box. That's a plus.

### What's running

Use `ais show performance` and its variations in combination with `ais show job` (and variations). The latter shows what's running in the cluster, and thus combining the two may make sense.

### See also:

* [AIS metrics](/docs/metrics.md)
* [Prometheus](/docs/prometheus.md)

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

## `ais show cluster`
Show cluster.

The command supports a variety of subcommands and scoping options:

```console
# ais show cluster <TAB-TAB>
bmd   config   proxy   smap   stats   target    [DAEMON_ID ...]
```

[Refer to `ais cluster` documentation for details and examples.](cluster.md#cluster-or-daemon-status)

## `ais show storage`
Show storage usage and utilization in the cluster. Show disks and mountpaths - for a single selected node or for all storage nodes.

When run with no subcommands, `ais show storage` defaults to `ais show storage disk`.

In addition, the command support the following subcommands:

```console
# ais show storage <TAB-TAB>
disk   mountpath   summary      [DAEMON_ID ...]
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

## `ais show job`
Show status of long-running jobs (aka [xactions](/docs/batch.md)). Choose from `download`, `dsort`, `xaction`, `etl`. Example:

```console
$ ais show job xaction t[ugoFtqUrrm]
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

For details and many more examples, please refer to:

- [`ais show job`](/docs/cli/job.md)
- [`ais show job dsort`](/docs/cli/dsort.md)
- [`ais show job download`](/docs/cli/download.md)
- [`ais show rebalance`](/docs/rebalance.md)
- [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges)
- [reading, writing, and listing archives](/docs/cli/object.md)
- [copying buckets](/docs/cli/bucket.md#copy-bucket)

---

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

