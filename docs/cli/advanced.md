---
layout: post
title: ADVANCED
permalink: /docs/cli/advanced
redirect_from:
 - /cli/advanced.md/
 - /docs/cli/advanced.md/
---

# `ais advanced` commands

Commands for special use cases (e.g. scripting) and *advanced* usage scenarios, whereby a certain level of understanding of possible consequences is implied and required:

```console
$ ais advanced --help
NAME:
   ais advanced - special commands intended for development and advanced usage

USAGE:
   ais advanced command [command options] [arguments...]

COMMANDS:
   gen-shards        generate and write random TAR shards, e.g.:
                     - gen-shards 'ais://bucket1/shard-{001..999}.tar' - write 999 random shards (default sizes) to ais://bucket1
                     - gen-shards 'gs://bucket2/shard-{01..20..2}.tgz' - 10 random gzipped tarfiles to Cloud bucket
                     (notice quotation marks in both cases)
   resilver          resilver user data on a given target (or all targets in the cluster): fix data redundancy
                     with respect to bucket configuration, remove migrated objects and old/obsolete workfiles
   preload           preload object metadata into in-memory cache
   remove-from-smap  immediately remove node from cluster map (advanced usage - potential data loss!)
   random-node       print random node ID (by default, random target)
   random-mountpath  print a random mountpath from a given target
   rotate-logs       rotate logs
```

AIS CLI features a number of miscellaneous and advanced-usage commands.

## Table of Contents
- [Manual Resilvering](#manual-resilvering)
- [Preload bucket](#preload-bucket)
- [Remove node from Smap](#remove-node-from-smap)

## Manual Resilvering

`ais advanced resilver [TARGET_ID]`

Start resilvering objects across all drives on one or all targets.
If `TARGET_ID` is specified, only that node will be resilvered. Otherwise, all targets will be resilvered.

### Examples

```console
$ ais advanced resilver # all targets will be resilvered
Started resilver "NGxmOthtE", use 'ais show job xaction NGxmOthtE' to monitor the progress

$ ais advanced resilver BUQOt8086  # resilver a single node
Started resilver "NGxmOthtE", use 'ais show job xaction NGxmOthtE' to monitor the progress
```

## Preload bucket

`ais advanced preload BUCKET`

Preload bucket's objects metadata into in-memory caches.

### Examples

```console
$ ais advanced preload ais://bucket
```

## Remove node from Smap

`ais advanced remove-from-smap NODE_ID`

Immediately remove node from the cluster map.

Beware! When the node in question is ais target, the operation may (and likely will) result in a data loss that cannot be undone. Use decommission and start/stop maintenance operations to perform graceful removal.

Any attempt to remove from the cluster map `primary` - ais gateway that currently acts as the primary (aka leader) - will fail.

### Examples

```console
$ ais show cluster proxy
PROXY            MEM USED %      MEM AVAIL       UPTIME
BcnQp8083        0.17%           31.12GiB        6m50s
xVMNp8081        0.16%           31.12GiB        6m50s
MvwQp8080[P]     0.18%           31.12GiB        6m40s
NnPLp8082        0.16%           31.12GiB        6m50s


$ ais advanced remove-from-smap MvwQp8080
Node MvwQp 8080 is primary: cannot remove

$ ais advanced remove-from-smap p[xVMNp8081]
$ ais show cluster proxy
PROXY            MEM USED %      MEM AVAIL       UPTIME
BcnQp8083        0.16%           31.12GiB        8m
NnPLp8082        0.16%           31.12GiB        8m
MvwQp8080[P]     0.19%           31.12GiB        7m50s
```


## Rotate logs: individual nodes or entire cluster

Usage: `ais advanced rotate-logs [NODE_ID]`

Example:

```console
$ ais show log t[kOktEWrTg]

Started up at 2023/11/07 18:06:22, host u2204, go1.21.1 for linux/amd64
W 18:06:22.930488 config:1713 load initial global config "/root/.ais1/ais.json"
...
...
```

Now, let's go ahead and rotate:

```console
$ ais advanced rotate-logs t[kOktEWrTg]
t[kOktEWrTg]: rotated logs

$ ais show log t[kOktEWrTg]
Rotated at 2023/11/07 18:07:31, host u2204, go1.21.1 for linux/amd64
Node t[kOktEWrTg], Version 3.21.1.69a90d64b, build time 2023-11-07T18:06:19-0500, debug false, CPUs(16, runtime=16)
...
```
