# Mountpath (disk) management

> A *mountpath* is a single disk **or** a volume (a RAID) formatted with a local filesystem of choice, **and** a local directory that AIS utilizes to store user data and AIS metadata. A mountpath can be disabled and (re)enabled, automatically or administratively, at any point during runtime. In a given cluster, a total number of mountpaths would normally compute as a direct product of (number of storage targets) x (number of disks in each target).

## Table of Contents
- [Show capacity usage](#show-capacity-usage)
- [Validate buckets](#validate-buckets)
- [Show mountpaths](#show-mountpaths)
- [Attach mountpath](#attach-mountpath)
- [Detach mountpath](#detach-mountpath)

## Show capacity usage

`ais storage summary [BUCKET | PROVIDER]`

Show summary information on a per bucket basis. If `BUCKET` is specified in the command line, the output gets narrowed down to this specific bucket. The same goes for `PROVIDER` (e.g., `ais://`, `aws://`, `gs://`, etc.) - again, only if specified.

> Bucket providers and supported remote backends are documented [here](/docs/providers.md).

Depending on the command line options (listed below), per-bucket information includes total number of objects, size of the bucket in bytes or megabytes, and percentage of the total capacity used by the bucket.

A recently added `--validate` option is intended to analyze integrity of the stored distributed content. The questions that we ask at validation time "cover" location of stored objects and their replicas, the number of replicas (and whether this number agrees with the bucket configuration), etc.

In particular, location of each objects stored in the cluster must at any point in time correspond to the current cluster map and, within each storage target, to the target's *mountpaths* (disks).  A failure to abide by location rules is called "misplacement"; misplaced objects - if any - must be migrated to their proper locations via automated processes called `global rebalance` and `resilver`:

* [global rebalance and reslver](/docs/rebalance.md)
* [resilvering selected targets: advanced usage](/docs/resourcesvanced.md)

When `--fast` option is used the summary will include (internal-usage) details such as temporary objects, copies, EC slices, metadata files, and more.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--fast` | `bool` | The option is designed primarily for internal usage. The output may not accurately reflect user-accessible content. | `false` |
| `--validate` | `bool` | Check objects for errors: misplaced, insufficient number of copies etc | `false` |
| `--cached` | `bool` | For buckets that have remote backend, list only objects stored in the cluster | `false` |

### Example

Show summary for all buckets.

```console
$ ais storage summary
NAME             OBJECTS         SIZE            USED %
ais://bck        2               59.24KiB        0.00%
```

Show estimated summary.

```console
$ ais storage summary --fast
NAME             EST. OBJECTS    EST. SIZE       EST. USED %
ais://bck        4               224.00KiB       0.00%
```

The bucket `ais://bck` has mirroring enabled, so its number of objects doubles.

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

## Show mountpaths

`ais storage mountpath show [DAEMON_ID]`

Show mountpaths for a given target or all targets.

> Note: Like many other `ais show` commands, `ais show mountpath` is aliased to `ais storage mountpath show` for ease of use.
> Both of these commands are used interchangeably throughout the documentation.

### Examples

```console
$ ais storage mountpath show 12367t8085
247389t8085
        Available:
			/tmp/ais/5/3
			/tmp/ais/5/1
        Disabled:
			/tmp/ais/5/2

$ ais storage mountpath show
247389t8085
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

`ais storage mountpath attach DAEMON_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Attach a mountpath on a specified target to AIS storage.

### Examples

```console
$ ais storage mountpath attach 12367t8080=/data/dir
```

## Detach mountpath

`ais storage mountpath detach DAEMON_ID=MOUNTPATH [DAEMONID=MOUNTPATH...]`

Detach a mountpath on a specified target from AIS storage.

### Examples

```console
$ ais storage mountpath detach 12367t8080=/data/dir
```
