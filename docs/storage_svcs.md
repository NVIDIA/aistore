## Table of Contents
- [Storage Services](#storage-services)
  - [Notation](#notation)
- [Checksumming](#checksumming)
- [LRU](#lru)
- [Erasure coding](#erasure-coding)
- [N-way mirror](#n-way-mirror)
  - [Read load balancing](#read-load-balancing)
  - [More examples](#more-examples)
- [Data redundancy: summary of the available options (and considerations)](#data-redundancy-summary-of-the-available-options-and-considerations)

## Storage Services

By default, buckets inherit [global configuration](/deploy/dev/local/aisnode_config.sh). However, several distinct sections of this global configuration can be overridden at startup or at runtime on a per bucket basis. The list includes checksumming, LRU, erasure coding, and local mirroring - please see the following sections for details.

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

## LRU

Overriding the global configuration can be achieved by specifying the fields of the `LRU` instance of the `LRUConf` struct that encompasses all LRU configuration fields.

* `lru.lowwm`: integer in the range [0, 100], representing the capacity usage low watermark
* `lru.highwm`: integer in the range [0, 100], representing the capacity usage high watermark
* `lru.atime_cache_max`: positive integer representing the maximum number of entries
* `lru.dont_evict_time`: string that indicates eviction-free period [atime, atime + dont]
* `lru.capacity_upd_time`: string indicating the minimum time to update capacity
* `lru.enabled`: bool that determines whether LRU is run or not; only runs when true

**NOTE**: In setting bucket properties for LRU, any field that is not explicitly specified defaults to the data type's zero value.

Example of setting bucket properties:

```console
$ ais bucket props <bucket-name> lru.lowwm=1 lru.highwm=100 lru.enabled=true
```

To revert bucket's entire configuration back to global (configurable) defaults, use `"action":"resetbprops"` with the same PATCH endpoint, e.g.:

```console
$ ais bucket props <bucket-name> --reset
```

In effect, resetting bucket properties is equivalent to populating all properties with the values from the corresponding sections of the [global configuration](/deploy/dev/local/aisnode_config.sh).

## Erasure coding

AIStore provides data protection that comes in several flavors: [end-to-end checksumming](#checksumming), [n-way mirroring](#n-way-mirror), replication (for *small* objects), and erasure coding.

Erasure coding, or EC, is a well-known storage technique that protects user data by dividing it into N fragments or slices, computing K redundant (parity) slices, and then storing the resulting (N+K) slices on (N+K) storage servers - one slice per target server.

EC schemas are flexible and user-configurable: users can select the N and the K (above), thus ensuring that user data remains available even if the cluster loses **any** (emphasis on the **any**) of its K servers.

A bucket inherits EC settings from global configuration. But it can be overridden on a per bucket basis.

* `ec.enabled`: bool - enables or disabled data protection the bucket
* `ec.data_slices`: integer in the range [2, 100], representing the number of fragments the object is broken into
* `ec.parity_slices`: integer in the range [2, 32], representing the number of redundant fragments to provide protection from failures. The value defines the maximum number of storage targets a cluster can lose but it is still able to restore the original object
* `ec.objsize_limit`: integer indicating the minimum size of an object that is erasure encoded. Smaller objects are just replicated.
* `ec.compression`: string that contains rules for LZ4 compression used by EC when it sends its fragments and replicas over network. Value "never" disables compression. Other values enable compression: it can be "always" - use compression for all transfers, or list of compression options, like "ratio=1.5" that means "disable compression automatically when compression ratio drops below 1.5"

Choose the number data and parity slices depending on the required level of protection and the cluster configuration. The number of storage targets must be greater than the sum of the number of data and parity slices. If the cluster uses only replication (by setting `objsize_limit` to a very high value), the number of storage targets must exceed the number of parity slices.

Rebalance supports erasure-coded buckets. Besides moving existing objects between targets, it repairs damaged objects and their slices if possible.

Notes:

- Every data and parity slice is stored on a separate storage target. To reconstruct a damaged object, AIStore requires at least `ec.data_slices` slices in total out of data and parity sets
- Small objects are replicated `ec.parity_slices` times to have the same level of data protection that big objects do
- Increasing the number of parity slices improves data protection level, but it may hit performance: doubling the number of slices approximately increases the time to encode the object by a factor of two

Example of setting bucket properties:

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
$ ais job start mirror --copies 1 ais://a
$ ais job start mirror --copies 2 ais://b
$ ais job start mirror --copies 3 ais://c
```

The operations (above) are in fact [extended actions](/xaction/README.md) that run asynchronously. Both Cloud and ais buckets are supported. You can monitor completion of those operations via generic [xaction API](/xaction/README.md).

Subsequently, all PUTs into an n-way configured bucket also generate **n** copies for all newly created objects. Which also goes to say that the ("makencopies") operation, in addition to creating or destroying replicas of existing objects will also automatically re-enable(if n > 1) or disable (if n == 1) mirroring as far as subsequent PUTs are concerned.

Note again that number of local replicas is defined on a per-bucket basis.

### Read load balancing
With respect to n-way mirrors, the usual pros-and-cons consideration boils down to (the amount of) utilized space, on the other hand, versus data protection and load balancing, on the other.

Since object replicas are end-to-end protected by [checksums](#checksumming) all of them and any one in particular can be used interchangeably to satisfy a GET request thus providing for multiple possible choices of local filesystems and, ultimately, local drives. Given n > 1, AIS will utilize the least loaded drive(s).

### More examples
The following sequence creates a bucket named `abc`, PUTs an object into it and then converts it into a 3-way mirror:

```console
$ ais bucket create abc
$ ais object put /tmp/obj1 ais://abc/obj1
$ ais job start mirror --copies 3 ais://abc
```

The next command will redefine the `abc` bucket created in the previous example as a 2-way mirror - all objects that were previously stored in three replicas will now have only two (replicas):

```console
$ ais job start mirror --copies 2 ais://abc
```

## Data redundancy: summary of the available options (and considerations)

Any of the supported options can be utilized at any time (and without downtime) - the list includes:

1. **cloud backend**  - [Backend Bucket](bucket.md#backend-bucket)
2. **mirroring** - [N-way mirror](#n-way-mirror)
3. **copying buckets**  - [Copy Bucket](/docs/cli/bucket.md#copy-bucket)
4. **erasure coding** - [Erasure coding](#erasure-coding)

For instance, you first could start with plain mirroring via `ais job start mirror BUCKET --copies N`, where N would be less or equal the number of target mountpaths (disks).

> It is generally assumed (and also strongly recommended) that all storage servers (targets) in AIS cluster have the same number of disks and are otherwise identical.

Copies will then be created on different disks of each storage target - for all already stored and future objects in a given bucket.

This option won't protect from node failures but it will provide a fairly good performance for writes and load balancing - for reads. As far as data redundancy, N-way mirror protects from failures of up to (N-1) disks in a storage server.

> It is the performance and the fact that probabilities of disk failures are orders of magnitude greater than node failures makes this "N-way mirror" option attractive, possibly in combination with periodic backups.

Further, you could at some point in time decide to associate a given AIS bucket with a Cloud (backend) bucket, thus making sure that your data is stored in one of the AIS-supported Clouds: Amazon S3, Google Cloud Storage, Azure Blob Storage.

Finally, you could erasure code (EC) a given bucket for `D + P` redundancy, where `D` and `P` are, respectively, the numbers of data and parity slices. For example:

```console
$ ais job start ec-encode -d 6 -p 4 abc
```

will erasure-code all objects in the `abc` bucket for the total of 10 slices stored on different AIS targets, plus 1 (one) full replica. In other words, this example requires at least `10 + 1 = 11` targets in the cluster.

> Generally, `D + P` erasure coding requires that AIS cluster has `D + P + 1` targets, or more.

> In addition to Reed-Solomon encoded slices, we currently always store a full replica - the strategy that uses available capacity but pays back with read performance.
