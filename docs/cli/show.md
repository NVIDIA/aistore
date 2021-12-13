---
layout: post
title: SHOW
permalink: /docs/cli/show
redirect_from:
 - /cli/show.md/
 - /docs/cli/show.md/
---

# CLI Reference for `show` commands

AIS CLI `show` command can universally be used to view summaries and details on cluster and its nodes, buckets and objects, running and finished jobs - in short, _all_ managed entities (see below). The command is a "hub" for all information-viewing commands that are currently supported.

Note that some of these commands (such as `ais show cluster` and `ais show job`) are aliased to their respective top-level commands for ease of use. This means that running `ais show cluster` is equivalent to running `ais cluster show`.

## Table of Contents
- [`ais show auth`](#ais-show-auth)
- [`ais show bucket`](#ais-show-bucket)
- [`ais show object`](#ais-show-object)
- [`ais show cluster`](#ais-show-cluster)
- [`ais show mountpath`](#ais-show-mountpath)
- [`ais show job`](#ais-show-job)
- [`ais show disk`](#ais-show-disk)
- [`ais show config`](#ais-show-config)
- [`ais show remote-cluster`](#ais-show-remote-cluster)
- [`ais show rebalance`](#ais-show-rebalance)
- [`ais show log`](#ais-show-log)

The following commands have aliases. In other words, they can be accessed through `ais show <command>` and also `ais <command> show`.

## `ais show auth`
The following subcommands are currently supported:

```console
   cluster  show registered clusters
   role     show existing user roles
   user     show users or user details
   config   show AuthN server configuration
```

[Refer to `ais auth` documentation for more.](auth.md#command-list)

## `ais show bucket`
Show bucket properties.

[Refer to `ais bucket` documentation for more.](bucket.md#show-bucket-properties)

## `ais show object`
Show object details.

[Refer to `ais object` documentation for more.](object.md#show-object-properties)

## `ais show cluster`
Show cluster and node details.

The command supports a variety of scoping options and (sub-command) specifiers:

```console
# ais show cluster <TAB-TAB>
bmd   config   proxy   smap   stats   target    [DAEMON_ID ...]
```

[Refer to `ais cluster` documentation for more.](cluster.md#cluster-or-daemon-status)

## `ais show mountpath`
Show mountpath list for targets.

[Refer to `ais mountpath` documentation for more.](mpath.md#show-mountpaths)

## `ais show job`
Show long-running jobs (aka [xactions](/docs/batch.md)). Example:

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

The following commands do not have any built-in aliases (in other words, they can only be accessed through `ais show <command>`).

## `ais show disk`
Show disk statistics for targets.

[Refer to `ais cluster` documentation for more.](cluster.md#show-disk-stats)

## `ais show config`
Show daemon configuration.

[Refer to `ais cluster` documentation for more.](config.md##show-configuration)

## `ais show remote-cluster`
Show information about attached AIS clusters.

[Refer to `ais cluster` documentation for more.](cluster.md#show-remote-clusters)

## `ais show rebalance`

Display details about the most recent rebalance xaction.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--refresh N` | `string` | watch the rebalance until it finishes or CTRL-C is pressed. Display the current stats every N seconds, where N ends with time suffix: s, m | ` ` |
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
