## Table of Contents

- [Storage Services](#storage-services)
  - [Notation](#notation)
- [Checksumming](#checksumming)
- [LRU and Space](#lru-and-space)
  - [Space watermarks](#space-watermarks)
  - [LRU configuration](#lru-configuration)
  - [Example setting space properties](#example-setting-space-properties)
  - [Example enabling LRU eviction for a given bucket](#example-enabling-lru-eviction-for-a-given-bucket)
- [Erasure coding](#erasure-coding)
  - [Example setting bucket properties](#example-setting-bucket-properties)
  - [Limitations](#limitations)
- [N-way mirror](#n-way-mirror)
  - [Read load balancing](#read-load-balancing)
  - [Another n-way example](#another-n-way-example)
- [Data redundancy: summary of the available options (and considerations)](#data-redundancy-summary-of-the-available-options-and-considerations)
- [Erasure-coding: with and without recovery](#erasure-coding-with-and-without-recovery)
  - [Example recovering lost or damaged slices and/or objects](#example-recovering-lost-or-damaged-slices-and-objects)
- [Chunking](#chunking)
  - [Storage layout](#storage-layout)
  - [Rechunk](#rechunk)
  - [Prefetch mechanism](#prefetch-mechanism)

## Storage Services

By default, buckets inherit [global configuration](/deploy/dev/local/aisnode_config.sh). However, several distinct sections of this global configuration can be overridden at startup or at runtime on a per bucket basis. The list includes checksumming, LRU, erasure coding, local mirroring, and chunking - please see the following sections for details.

### Notation

In this document, `G` - denotes a (hostname:port) pair of any gateway in the AIS cluster.

## Checksumming

All cluster and object-level metadata is protected by checksums. Secondly, unless user explicitly disables checksumming for a given bucket, all user data stored in this bucket is also protected.

For detailed overview, theory of operations, and supported checksumms, please see this [document](checksum.md).

Example: configuring checksum properties for a bucket:

```console
$ ais bucket props <bucket-name> checksum.validate_cold_get=true checksum.validate_warm_get=false checksum.type=xxhash checksum.enable_read_range=false
```

For more examples, please to refer to [supported checksums and brief theory of operations](checksum.md).

## LRU and Space

LRU (Least Recently Used) configuration contains the following 3 (three) knobs:

```console
$ ais config cluster lru

PROPERTY                 VALUE
lru.dont_evict_time      2h0m
lru.capacity_upd_time    10m
lru.enabled              true
```

Most importantly, cluster-wide default in the example above is `true`.
What it means is that every newly added remote bucket will be "evictable` upon reaching certain space-utilization threshold.

Speaking of which, here's the Space section of the configuration (and again, the actual values below are the defaults that we currently have for [local playground](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#local-playground):

```console
$ ais config cluster space --json

    "space": {
        "cleanupwm": 65,
        "lowwm": 75,
        "highwm": 90,
        "out_of_space": 95
    }
```

Note that "space" watermarks (`space.cleanupwm`, `space.lowwm`, `space.highwm`, and `space.out_of_space`) apply to the entire cluster, not to individual buckets.

On the other hand, LRU can be configured on a per-bucket basis:

* [example enabling LRU eviction for a given bucket](#example-enabling-lru-eviction-for-a-given-bucket)

### Space watermarks

* `space.cleanupwm`: integer in the range `[0, 100]`, used capacity (%) that triggers cleanup (deleted objects and buckets, extra copies, etc.) (storage cleanup watermark %)
* `space.lowwm`: integer in the range `[0, 100]`, if filesystem usage exceeds `highwm` (high watermark %) LRU tries to evict objects so the filesystem usage drops to `lowwm` (low watermark %)
* `space.highwm`: integer in the range `[0, 100]`, LRU starts immediately if a filesystem usage exceeds the value representing `highwm` (high watermark %)
* `space.out_of_space`: integer in the range `[0, 100]`, `out_of_space` (%) if exceeded, the target starts failing new PUTs and keeps failing them until its local used-cap gets back below `highwm`

See also:

* [example setting space properties](#example-setting-space-properties)

### LRU configuration

* `lru.dont_evict_time`: string that indicates eviction-free period `[atime, atime + dont]`
* `lru.capacity_upd_time`: string indicating the minimum time to update capacity
* `lru.enabled`: bool that determines whether LRU is run or not; only runs when true

Note the one, maybe subtle, difference between `ais://` buckets and remote buckets (the latter including, of course, Cloud buckets):

> LRU enabled/disabled default in the cluster config only affects remote buckets - buckets that, effectively, have a backup. For in-cluster `ais://` buckets LRU is always by default disabled (and "lru.enabled" knob from the cluster configuration is ignored).

You can still enable LRU for the `ais://` buckets but that must be done explicitly:

* [example enabling LRU eviction for a given bucket](#example-enabling-lru-eviction-for-a-given-bucket)

### Example setting space properties

```console
$ ais config cluster space.cleanupwm=40 lru.enabled=true space.lowwm=45 space.highwm=47.15 lru.dont_evict_time=1s
```

### Example enabling LRU eviction for a given bucket

```console
$ ais create ais://nnn
"ais://nnn" created

$ ais bucket props <TAB-TAB>
set     reset   show

$ ais bucket props set ais://nnn lru.enabled
PROPERTY         VALUE
lru.enabled      false

$ ais bucket props set ais://nnn lru.enabled true
"lru.enabled" set to: "true" (was: "false")

Bucket props successfully updated.
```

## Erasure coding

AIStore provides data protection that comes in several flavors: [end-to-end checksumming](#checksumming), [n-way mirroring](#n-way-mirror), replication (for *small* objects), and erasure coding.

Erasure coding, or EC, is a well-known storage technique that protects user data by dividing it into D fragments or slices, computing P redundant (parity) slices, and then storing the resulting (D+P) slices on (D+P) storage servers - one slice per target server.

EC schemas are flexible and user-configurable: users can select the D and the P (above), thus ensuring that user data remains available even if the cluster loses **any** (emphasis on the **any**) of its P servers.

A bucket inherits EC settings from global configuration. But it can be overridden on a per bucket basis.

```console
$ ais start ec-encode --help
NAME:
   ais start ec-encode - erasure code entire bucket, e.g.:
     - 'ais start ec-encode ais://nnn -d 8 -p 2'                          - erasure-code ais://nnn for 8 data and 2 parity slices;
     - 'ais start ec-encode ais://nnn --data-slices 8 --parity-slices 2'  - same as above;
     - 'ais start ec-encode ais://nnn --recover'                          - check and make sure that every ais://nnn object is properly erasure-coded.
   see also: 'ais start mirror'

USAGE:
   ais start ec-encode BUCKET [command options]

OPTIONS:
   --data-slices value, -d value    number of data slices (default: 2)
   --parity-slices value, -p value  number of parity slices (default: 2)
   --non-verbose, --nv              non-verbose (quiet) output, minimized reporting, fewer warnings
   --recover                        check and make sure that each and every object is properly erasure coded
   --help, -h                       show help
```

* `ec.enabled`: bool - enables or disabled data protection the bucket
* `ec.data_slices`: integer in the range [2, 100], representing the number of fragments the object is broken into
* `ec.parity_slices`: integer in the range [2, 32], representing the number of redundant fragments to provide protection from failures. The value defines the maximum number of storage targets a cluster can lose but it is still able to restore the original object
* `ec.objsize_limit`: integer indicating the minimum size of an object that is erasure encoded. Smaller objects are just replicated.
* `ec.compression`: string that contains rules for LZ4 compression used by EC when it sends its fragments and replicas over network. Value "never" disables compression. Other values enable compression: it can be "always" - use compression for all transfers, or list of compression options, like "ratio=1.5" that means "disable compression automatically when compression ratio drops below 1.5"

Choose the number data and parity slices depending on the required level of protection and the cluster configuration.

The number of storage targets must be greater than the sum of the number of data and parity slices. If the cluster uses only replication (by setting `objsize_limit` to a very high value), the number of storage targets must exceed the number of parity slices.

Global rebalance supports erasure-coded buckets.

**Notes**:

> Every data and parity slice is stored on a separate storage target. To reconstruct a damaged object, AIStore requires at least `ec.data_slices` slices in total out of data and parity sets
> Small objects are replicated `ec.parity_slices` times to have the same level of data protection that big objects do
> Increasing the number of parity slices improves data protection level, but it may hit performance: doubling the number of slices approximately increases the time to encode the object by a factor of two

### Example setting bucket properties

```console
$ ais bucket props ais://<bucket-name> lru.lowwm=1 lru.highwm=90 ec.enabled=true ec.data_slices=4 ec.parity_slices=2
```

To change only one EC property(e.g, enable or disable EC for a bucket) without touching other bucket properties, use the single set property API. Example of disabling EC:

```console
$ ais bucket props ais://<bucket-name> ec.enabled=true
```

or using AIS CLI utility:

enable EC for a bucket with custom number of data and parity slices. It should be done using 2 commands: the first one changes the numbers while EC is disabled, and the second one enables EC with new slice count:

```console
$ ais bucket props mybucket ec.data_slices=3 ec.parity_slices=3
$ ais bucket props mybucket ec.enabled=true
```

check that EC properties are applied:

```console
$ ais show bucket mybucket ec
PROPERTY	 VALUE
ec		 3:3 (256KiB)
```

### Limitations

Once a bucket is configured for EC, it'll stay erasure coded for its entire lifetime - there is currently no supported way to change this once-applied configuration to a different (N, K) schema, disable EC, and/or remove redundant EC-generated content.

Only option `ec.objsize_limit` can be changed if EC is enabled. Modifying this property requires `force` flag to be set.

Note that after changing any EC option the cluster does not re-encode existing objects. The existing objects are rebuilt only after the objects are changed(rename, put new version etc).

## N-way mirror

Yet another supported storage service is n-way mirroring providing for bucket-level data redundancy and data protection. The service makes sure that each object in a given distributed (local or Cloud) bucket has exactly **n** object replicas, where n is an arbitrary user-defined integer greater or equal 1.

In other words, AIS n-way mirroring is intended to withstand loss of disks, not storage nodes (aka AIS targets).

> For the latter, please consider using #erasure-coding and/or any of the alternative backup/restore mechanisms.

The service ensures is that for any given object there will be *no two replicas* sharing the same local disk.

> Unlike [erasure coding](#erasure-coding) that takes care of distributing redundant content across *different* clustered nodes, local mirror is, as the name implies, local. When a bucket is [configured as a mirror](/deploy/dev/local/aisnode_config.sh), objects placed into this bucket get locally replicated and the replicas are stored in local filesystems.

> As aside, note that AIS storage targets can be deployed to utilize Linux LVMs that provide a variety of RAID/mirror schemas.

The following example configures buckets a, b, and c to store n = 1, 2, and 3 object replicas, respectively:

```console
$ ais start mirror --copies 1 ais://a
$ ais start mirror --copies 2 ais://b
$ ais start mirror --copies 3 ais://c
```

The operations (above) are in fact [extended actions](https://github.com/NVIDIA/aistore/blob/main/xact/README.md) that run asynchronously. Both Cloud and ais buckets are supported. You can monitor completion of those operations via generic [xaction API](/api/xaction.go).

Subsequently, all PUTs into an n-way configured bucket also generate **n** copies for all newly created objects. Which also goes to say that the ("make-n-copies") operation, in addition to creating or destroying replicas of existing objects will also automatically re-enable(if n > 1) or disable (if n == 1) mirroring as far as subsequent PUTs are concerned.

Note again that number of local replicas is defined on a per-bucket basis.

### Read load balancing
With respect to n-way mirrors, the usual pros-and-cons consideration boils down to (the amount of) utilized space, on the other hand, versus data protection and load balancing, on the other.

Since object replicas are end-to-end protected by [checksums](#checksumming) all of them and any one in particular can be used interchangeably to satisfy a GET request thus providing for multiple possible choices of local filesystems and, ultimately, local drives. Given n > 1, AIS will utilize the least loaded drive(s).

## Another n-way example
The following sequence creates a bucket named `abc`, PUTs an object into it and then converts it into a 3-way mirror:

```console
$ ais create ais://abc
$ ais put /tmp/obj1 ais://abc/obj1
$ ais start mirror --copies 3 ais://abc
```

The next command will redefine the `abc` bucket created in the previous example as a 2-way mirror - all objects that were previously stored in three replicas will now have only two (replicas):

```console
$ ais start mirror --copies 2 ais://abc
```

## Data redundancy: summary of the available options (and considerations)

Any of the supported options can be utilized at any time (and without downtime) - the list includes:

1. **cloud backend**  - [Backend Bucket](/docs/bucket.md#backend-buckets)
2. **mirroring** - [N-way mirror](#n-way-mirror)
3. **copying**  - [Copy (list, range, and/or prefix) selected objects or entire (in-cluster or remote) buckets](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)
4. **erasure coding** - [Erasure coding](#erasure-coding)

For instance, you first could start with plain mirroring via `ais start mirror BUCKET --copies N`, where N would be less or equal the number of target mountpaths (disks).

> It is generally assumed (and also strongly recommended) that all storage servers (targets) in AIS cluster have the same number of disks and are otherwise identical.

Copies will then be created on different disks of each storage target - for all already stored and future objects in a given bucket.

This option won't protect from node failures but it will provide a fairly good performance for writes and load balancing - for reads. As far as data redundancy, N-way mirror protects from failures of up to (N-1) disks in a storage server.

> It is the performance and the fact that probabilities of disk failures are orders of magnitude greater than node failures makes this "N-way mirror" option attractive, possibly in combination with periodic backups.

Further, you could at some point in time decide to associate a given AIS bucket with a Cloud (backend) bucket, thus making sure that your data is stored in one of the AIS-supported Clouds: Amazon S3, Google Cloud Storage, Azure Blob Storage, and/or Oracle (OCI) Object Storage.

Finally, you could erasure code (EC) a given bucket for `D + P` redundancy, where `D` and `P` are, respectively, the numbers of data and parity slices:

```console
$ ais start ec-encode --help
NAME:
   ais start ec-encode - erasure code entire bucket, e.g.:
     - 'ais start ec-encode ais://nnn -d 8 -p 2'                          - erasure-code ais://nnn for 8 data and 2 parity slices;
     - 'ais start ec-encode ais://nnn --data-slices 8 --parity-slices 2'  - same as above;
     - 'ais start ec-encode ais://nnn --recover'                          - check and make sure that every ais://nnn object is properly erasure-coded.
   see also: 'ais start mirror'

USAGE:
   ais start ec-encode BUCKET [command options]

OPTIONS:
   --data-slices value, -d value    number of data slices (default: 2)
   --parity-slices value, -p value  number of parity slices (default: 2)
   --non-verbose, --nv              non-verbose (quiet) output, minimized reporting, fewer warnings
   --recover                        check and make sure that each and every object is properly erasure coded
   --help, -h                       show help
```

## Erasure-coding: with and without recovery

Assuming, ais://abc is not erasure-coded (or its erasure-coding property is disabled):

```console
$ ais start ec-encode ais://abc -d 8 -p 2

## or, same:
##
$ ais start ec-encode ais://abc --data-slices 8 --parity-slices 2
```

This will erasure-code all objects in the `ais://abc` bucket for the total of 10 slices stored on different AIS targets, plus 1 (one) full replica. In other words, this example requires at least `10 + 1 = 11` targets in the cluster.

> Generally, `D + P` erasure coding requires that AIS cluster has `D + P + 1` targets, or more.

> In addition to Reed-Solomon encoded slices, we currently always store a full replica - the strategy that uses available capacity but pays back with read performance.

### Example recovering lost or damaged slices and objects

But what if there's an accident that involves corrupted or deleted data, lost disks, and/or entire nodes?

Well, erasure-coding supports a special _recovery_ mode to "check and make sure that each and every object is properly erasure coded."

```console
$ ais start ec-encode ais://abc --recover

## or same, assuming the bucket is (D=8, P=2) erasure-coded:
##
$ ais start ec-encode ais://abc --data-slices 8 --parity-slices 2
```

## Chunking

A bucket's `chunks` configuration determines how its objects are stored: as single contiguous files (monolithic) or as chunks described by a chunk manifest. As with [erasure coding](#erasure-coding) and [n-way mirroring](#n-way-mirror), the bucket configuration is the rule: it is the single record of the bucket's intended storage layout, and cluster-administrative jobs that operate on many objects converge the data to it rather than override it.

The relevant properties are:

| Property | Meaning |
|---|---|
| `chunks.objsize_limit` | Auto-chunking threshold; `0` disables auto-chunking (soft limit) |
| `chunks.max_monolithic_size` | Maximum size of a monolithic object; cannot be disabled (hard limit) |
| `chunks.chunk_size` | Chunk size used whenever the bucket's configuration requires chunking |

Explicit per-object overrides remain supported: client-side multipart PUT, S3 multipart upload (stored using the client's part sizes), and a single-object blob download with an explicitly specified chunk size. Each is a deliberate, advanced choice scoped to one object.

### Storage layout

For a given object size:

1. Above `chunks.max_monolithic_size`: chunked, with `chunks.chunk_size`.
2. Otherwise, when auto-chunking is enabled and the size is at or above `chunks.objsize_limit`: chunked, with `chunks.chunk_size`.
3. Otherwise: monolithic - except for the explicit per-object overrides above and the prefetch exception [below](#prefetch-mechanism).

A layout rule is independent of how the object arrives. It applies equally to PUT, cold GET, copy, and rechunk.

> **Status (v5.1):** rule 1 is enforced by PUT, cold GET, and copy; rechunk enforces it only for the objects it rewrites - an existing monolithic object above `chunks.max_monolithic_size` (e.g., after lowering it) is left as is. Rule 2 is currently enforced by rechunk only; PUT and cold GET do not yet auto-chunk at `chunks.objsize_limit`.

### Rechunk

`ais bucket rechunk` converges existing objects to the rules above. To change a bucket's layout, update its properties first, then run the job:

```console
$ ais bucket props set ais://abc chunks.chunk_size=16MiB chunks.objsize_limit=50MiB
$ ais bucket rechunk ais://abc
```

The `--prefix` option restricts the job to a subset of objects, for incremental conversion - not to apply a different policy to part of the bucket.

Per-job `--chunk-size` and `--objsize-limit` overrides are deprecated as of v5.1 and planned for removal in v5.2; see [v5.1 release notes](/docs/relnotes/5.1.md#deprecated-apis).

### Prefetch mechanism

Prefetch selects, per object, how to fetch it from the remote backend - a regular cold GET, or the blob downloader, which retrieves byte ranges in parallel. The prefetch `blob-threshold` expresses that intent; it does not prescribe storage layout.

Planned for v5.2:

- `blob-threshold` == 0: regular cold GET.
- With bucket auto-chunking enabled: blob download only at or above max(`blob-threshold`, `chunks.objsize_limit`), using `chunks.chunk_size`.
- With auto-chunking disabled: blob download at or above `blob-threshold`, producing a chunked object; the job warns once.
- A regular cold GET can still store the result chunked when the [storage layout](#storage-layout) rules require it.

The third case is a deliberate exception to rule 3. A `chunks.objsize_limit` of zero does express a layout preference - automatic writes remain monolithic - but reassembling parallel-fetched ranges into a monolithic file degrades performance. Prefetch therefore keeps the chunked result, uses the prefetch `blob-chunk-size` (or the blob-downloader default), and suggests `ais bucket rechunk`, which restores such objects to monolithic form per the bucket's configuration.

> **Status (v5.1):** prefetch uses the blob downloader at or above `blob-threshold` regardless of bucket configuration, with `blob-chunk-size` or the blob-downloader default.
