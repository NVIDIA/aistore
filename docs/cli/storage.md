---
layout: post
title: STORAGE
permalink: /docs/cli/storage
redirect_from:
 - /cli/storage.md/
 - /docs/cli/storage.md/
---

`ais storage` command supports the following subcommands:

```console
$ ais storage <TAB-TAB>
cleanup     disk        mountpath   summary     validate
```

Alternatively (or in addition), run with `--help` to view subcommands and short descriptions, both:

```console
$ ais storage --help
NAME:
   ais storage - monitor and manage clustered storage

USAGE:
   ais storage command [command options] [arguments...]

COMMANDS:
   show       show storage usage and utilization, disks and mountpaths
   summary    show bucket sizes and %% of used capacity on a per-bucket basis
   validate   check buckets for misplaced objects and objects that have insufficient numbers of copies or EC slices
   mountpath  show and attach/detach target mountpaths
   disk       show disk utilization and read/write statistics
   cleanup    perform storage cleanup: remove deleted objects and old/obsolete workfiles

OPTIONS:
   --help, -h  show help
```

As always, each subcommand (above) will have its own help and usage examples - the latter possibly spread across multiple markdowns.

> You can easily look up examples and descriptions of any keyword via a simple `find`, for instance:

```console
$ find . -type f -name "*.md" | xargs grep "ais.*mountpath"
```

## Table of Contents
- [Storage cleanup](#storage-cleanup)
- [Show capacity usage](#show-capacity-usage)
- [Validate buckets](#validate-buckets)
- [Mountpath (and disk) management](#mountpath-and-disk-management)
- [Show mountpaths](#show-mountpaths)
- [Attach mountpath](#attach-mountpath)
- [Detach mountpath](#detach-mountpath)

## Storage cleanup

```console
$ ais storage cleanup --help
NAME:
   ais storage cleanup - perform storage cleanup: remove deleted objects and old/obsolete workfiles

USAGE:
   ais storage cleanup [command options] PROVIDER:[//BUCKET_NAME]

OPTIONS:
   --force, -f      disregard interrupted rebalance and possibly other conditions preventing full cleanup
                    (tip: check 'ais config cluster lru.dont_evict_time' as well)
   --wait           wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --timeout value  maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --help, -h       show help
```

Similar to all supported batch operations (aka `xactions`), cleanup runs asynchronously and can be monitored during its run, e.g.:

```console
# ais storage cleanup
Started storage cleanup "BlpmlObF8", use 'ais job show xaction BlpmlObF8' to monitor the progress
```

Further references:

* [Batch operations](/docs/batch.md)
* [`ais show job`](/docs/cli/job.md)

## Show capacity usage

For command line options and usage examples, please refer to:

* [bucket summary](/docs/cli/bucket.md#show-bucket-summary)

## Validate buckets

`ais storage validate [BUCKET | PROVIDER]`

Checks all objects of the bucket `BUCKET` and show the number of found issues:
the number of misplaced objects, the number of objects that have insufficient number of copies etc.
Non-zero number of misplaced objects may mean a bucket needs rebalancing.

If the optional argument is omitted, show information about all buckets.

Because the command checks every object, it may take a lot of time for big buckets.
It is recommended to set bucket name or provider name to decrease execution time.

### Example

Validate only AIS buckets

```
$ ais storage validate  ais://
BUCKET            OBJECTS         MISPLACED       MISSING COPIES
ais://bck1        2               0               0
ais://bck2        3               1               0
```

The bucket `ais://bck2` has 3 objects and one of them is misplaced, i.e. it is inaccessible by a client.
It results in `ais ls ais://bck2` returns only 2 objects.

## Mountpath (and disk) management

There are two related commands:

* `ais storage disk`
* `ais storage mountpath`

where `mountpath` is a higher-level abstraction that typically "utilizes" a single undivided disk. More exactly:

> A *mountpath* is a single disk **or** a volume (a RAID) formatted with a local filesystem of choice, **and** a local directory that AIS utilizes to store user data and AIS metadata. A mountpath can be disabled and (re)enabled, automatically or administratively, at any point during runtime. In a given cluster, a total number of mountpaths would normally compute as a direct product of (number of storage targets) x (number of disks in each target).

You can manage and monitor (i.e., `show`) disks and mountpaths using `ais storage` command.

> For strictly monitoring purposes, you can universally use `ais show` command, e.g.: `ais show storage disk`, etc.

## Show disks

`ais storage disk show [TARGET_ID]`

or, same:

`ais show storage disk [TARGET_ID]`

## Show mountpaths

As the name implies, the syntax:

`ais show storage mountpath [TARGET_ID]`

for example:

```console
$ ais show storage mountpath t[TqPtghbiRw]

TqPtghbiRw
        Used Capacity (all disks): avg 15% max 18%
                                                /ais/mp1/2 /dev/nvme0n1(xfs)
                                                /ais/mp2/2 /dev/nvme1n1(xfs)
                                                /ais/mp3/2 /dev/nvme2n1(xfs)
                                                /ais/mp4/2 /dev/nvme3n1(xfs)
```

As always, `--help` will also list supported options. Note in particular the option to run continuously and periodically:

```console
$ ais show storage mountpath --help
NAME:
   ais show storage mountpath - show target mountpaths

USAGE:
   ais show storage mountpath [command options] [TARGET_ID]

OPTIONS:
   --refresh value  interval for continuous monitoring;
                    valid time units: ns, us (or µs), ms, s (default), m, h
   --count value    used together with '--refresh' to limit the number of generated reports (default: 0)
   --json, -j       json input/output
   --help, -h       show help
```

Show mountpaths for a given target or all targets.

> **Ease of Usage** notice: like all other `ais show` commands, `ais show storage mountpath` is an alias (or a shortcut) - in this specific case - for `ais storage mountpath show`.

### Examples (_slightly outdated_)

```console
$ ais storage mountpath show 12356t8085
247389t8085
        Available:
			/tmp/ais/5/3
			/tmp/ais/5/1
        Disabled:
			/tmp/ais/5/2

$ ais storage mountpath show
12356t8085
        Available:
			/tmp/ais/5/3
			/tmp/ais/5/1
        Disabled:
			/tmp/ais/5/2
147665t8084
        Available:
			/tmp/ais/4/3
			/tmp/ais/4/1
			/tmp/ais/4/2
426988t8086
	No mountpaths
```

## Attach mountpath

`ais storage mountpath attach TARGET_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Attach a mountpath on a specified target to AIS storage.

### Examples

```console
$ ais storage mountpath attach 12367t8080=/data/dir
```

## Detach mountpath

`ais storage mountpath detach TARGET_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Detach a mountpath on a specified target from AIS storage.

### Examples

```console
$ ais storage mountpath detach 12367t8080=/data/dir
```
