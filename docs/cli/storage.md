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
- [Validate in-cluster content for misplaced objects and missing copies](#validate-in-cluster-content-for-misplaced-objects-and-missing-copies)
- [Mountpath (and disk) management](#mountpath-and-disk-management)
- [Show mountpaths](#show-mountpaths)
- [Attach mountpath](#attach-mountpath)
- [Detach mountpath](#detach-mountpath)

## Storage cleanup

```console
$ ais storage cleanup --help
NAME:
   ais storage cleanup - remove deleted objects and old/obsolete workfiles; remove misplaced objects; optionally, remove zero size objects

USAGE:
   ais storage cleanup [command options] PROVIDER:[//BUCKET_NAME]

OPTIONS:
   --force, -f      disregard interrupted rebalance and possibly other conditions preventing full cleanup
                    (tip: check 'ais config cluster lru.dont_evict_time' as well)
   --rm-zero-size   remove zero-size objects (caution: advanced usage only)
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

## Validate in-cluster content for misplaced objects and missing copies

```console
$ ais scrub --help

NAME:
   ais scrub - (alias for "storage validate") Check in-cluster content for misplaced objects, objects that have insufficient numbers of copies, zero size, and more
   e.g.:
     * ais storage validate                 - validate all in-cluster buckets;
     * ais scrub                            - same as above;
     * ais storage validate ais             - validate (a.k.a. scrub) all ais:// buckets;
     * ais scrub s3                         - ditto, all s3:// buckets;
     * ais scrub s3 --refresh 10            - same as above while refreshing runtime counter(s) every 10s;
     * ais scrub gs://abc/images/           - validate part of the gcp bucket under 'images/`;
     * ais scrub gs://abc --prefix images/  - same as above.

USAGE:
   ais scrub [command options] [BUCKET[/PREFIX]] [PROVIDER]

OPTIONS:
   --all-columns          Show all columns, including those with only zero values
   --cached               Only visit in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   --count value          Used together with '--refresh' to limit the number of generated reports, e.g.:
                           '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --large-size value     Count and report all objects that are larger or equal in size  (e.g.: 4mb, 1MiB, 1048576, 128k; default: 5 GiB)
   --limit value          The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                          e.g.:
                          - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                          - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                          - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/-- (default: 0)
   --max-pages value      Maximum number of pages to display (see also '--page-size' and '--limit')
                          e.g.: 'ais ls az://abc --paged --page-size 123 --max-pages 7 (default: 0)
   --no-headers, -H       Display tables without headers
   --non-recursive, --nr  Non-recursive operation, e.g.:
                          - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                          - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                          - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                          - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --page-size value      Maximum number of object names per page; when the flag is omitted or 0
                          the maximum is defined by the corresponding backend; see also '--max-pages' and '--paged' (default: 0)
   --prefix value         For each bucket, select only those objects (names) that start with the specified prefix, e.g.:
                          '--prefix a/b/c' - sum up sizes of the virtual directory a/b/c and objects from the virtual directory
                          a/b that have names (relative to this directory) starting with the letter c
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --small-size value     Count and report all objects that are smaller or equal in size (e.g.: 4, 4b, 1k, 128kib; default: 0)
   --help, -h             Show help
```

Checks all objects of the bucket `BUCKET` and show number of misplaced objects, number of objects that have insufficient number of copies, etc.

If optional arguments are omitted, show information about all in-cluster buckets.

### Example: validate a given prefix-defined portion of an  s3 bucket

```console
$ ais scrub s3://data/my-prefix --large-size 500k

BUCKET/PREFIX        OBJECTS         NOT-CACHED      SMALL   LARGE           VER-CHANGED     DELETED
s3://data/my-prefix  1637 (1.6GiB)   1465 (1.4GiB)   -       172 (172.0MiB)  1 (1.0MiB)      1 (1.0MiB)

Detailed Logs
-------------
* not-cached objects:   /tmp/.ais-scrub-not-cached.204f71.log (1465 records)
* large objects:        /tmp/.ais-scrub-large.204f71.log (172 records)
* ver-changed objects:  /tmp/.ais-scrub-ver-changed.204f71.log (1 record)
* deleted objects:      /tmp/.ais-scrub-deleted.204f71.log (1 record)
```

### Example: same as above but show all columns

In other words, include relevant metrics that have only zero values.

```console
$ ais scrub s3://data/my-prefix --large-size 500k --all-columns

BUCKET/PREFIX        OBJECTS         NOT-CACHED      MISPLACED(cluster)  MISPLACED(mountpath) MISSING-COPIES  SMALL  LARGE          VER-CHANGED   DELETED
s3://data/my-prefix  1637 (1.6GiB)   1465 (1.4GiB)   -                   -                    -               -      172 (172.0MiB) 1 (1.0MiB)    1 (1.0MiB)

Detailed Logs
-------------
* not-cached objects:   /tmp/.ais-scrub-not-cached.204f8c.log (1465 records)
* large objects:        /tmp/.ais-scrub-large.204f8c.log (172 records)
* ver-changed objects:  /tmp/.ais-scrub-ver-changed.204f8c.log (1 record)
* deleted objects:      /tmp/.ais-scrub-deleted.204f8c.log (1 record)
```

Note that 172 (records) = 1637 - 1465.

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
