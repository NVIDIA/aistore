## Table of Contents
- [Storage Services](#storage-services)
   - [Notation](#notation)
- [Checksumming](#checksumming)
- [LRU](#lru)
- [Erasure coding](#erasure-coding)
- [N-way mirror](#n-way-mirror)
   - [Read load balancing](#read-load-balancing)
   - [More examples](#more-examples)

## Storage Services

By default, buckets inherit [global configuration](/ais/setup/config.sh). However, several distinct sections of this global configuration can be overridden at startup or at runtime on a per bucket basis. The list includes checksumming, LRU, erasure coding, and local mirroring - please see the following sections for details.

### Notation

In this document, `G` - denotes a (hostname:port) pair of any gateway in the AIS cluster.

## Checksumming

Checksumming on bucket level is configured by setting bucket properties:

* `cksum.type`: `"none"`,`"xxhash"` or `"inherit"` configure hashing type. Value
`"inherit"` indicates that the global checksumming configuration should be used.
* `cksum.validate_cold_get`: `true` or `false` indicate
whether to perform checksum validation during cold GET.
* `cksum.validate_warm_get`: `true` or `false` indicate
whether to perform checksum validation during warm GET.
* `cksum.enable_read_range`: `true` or `false` indicate whether to perform checksum validation during byte serving.

Value for the `type` field (see above) *must* be provided *every* time the bucket properties are updated, otherwise the request will be rejected.

Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"cksum": {"type": "xxhash", "validate_cold_get": true, "validate_warm_get": false, "enable_read_range": false}}}' 'http://G/v1/buckets/<bucket-name>'
```

## LRU

Overriding the global configuration can be achieved by specifying the fields of the `LRU` instance of the `lruconfig` struct that encompasses all LRU configuration fields.

* `lru.lowwm`: integer in the range [0, 100], representing the capacity usage low watermark
* `lru.highwm`: integer in the range [0, 100], representing the capacity usage high watermark
* `lru.atime_cache_max`: positive integer representing the maximum number of entries
* `lru.dont_evict_time`: string that indicates eviction-free period [atime, atime + dont]
* `lru.capacity_upd_time`: string indicating the minimum time to update capacity
* `lru.enabled`: bool that determines whether LRU is run or not; only runs when true

**NOTE**: In setting bucket properties for LRU, any field that is not explicitly specified is defaulted to the data type's zero value.

Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops","value":{"cksum":{"type":"none","validate_cold_get":true,"validate_warm_get":true,"enable_read_range":true},"lru":{"lowwm":1,"highwm":100,"atime_cache_max":1,"dont_evict_time":"990m","capacity_upd_time":"90m","enabled":true}}}' 'http://G/v1/buckets/<bucket-name>'
```

To revert a bucket's entire configuration back to use global parameters, use `"action":"resetprops"` to the same PUT endpoint as above as such:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"resetprops"}' 'http://G/v1/buckets/<bucket-name>'
```
### LRU for ais buckets

By default, LRU eviction is only enabled for cloud buckets. To enable automated eviction for ais buckets, set `lru.local_buckets` to true in [config.sh](/ais/setup/config.sh) before deploying AIS. Note that this is for advanced usage only, since this causes automatic deletion of objects in ais buckets, and therefore can cause data to be gone forever if not backed up outside of AIS.

## Erasure coding

AIStore provides data protection that comes in several flavors: [end-to-end checksumming](#checksumming), [n-way mirroring](#n-way-mirror), replication (for *small* objects), and erasure coding.

Erasure coding, or EC, is a well-known storage technique that protects user data by dividing it into N fragments or slices, computing K redundant (parity) slices, and then storing the resulting (N+K) slices on (N+K) storage servers - one slice per target server.

EC schemas are flexible and user-configurable: users can select the N and the K (above), thus ensuring that user data remains available even if the cluster loses **any** (emphasis on the **any**) of its K servers.

A bucket inherits EC settings from global configuration. But it can be overridden on a per bucket basis.

* `ec.enabled`: bool - enables or disabled data protection the bucket
* `ec.data_slices`: integer in the range [2, 100], representing the number of fragments the object is broken into
* `ec.parity_slices`: integer in the range [2, 32], representing the number of redundant fragments to provide protection from failures. The value defines the maximum number of storage targets a cluster can lose but it is still able to restore the original object
* `ec.objsize_limit`: integer indicating the minimum size of an object that is erasure encoded. Smaller objects are just replicated. The field can be 0 - in this case the default value is used (as of version 1.3 it is 256KiB)
* `ec.compression`: string that contains rules for LZ4 compression used by EC when it sends its fragments and replicas over network. Value "never" disables compression. Other values enable compression: it can be "always" - use compression for all transfers, or list of compression options, like "ratio=1.5" that means "disable compression automatically when compression ratio drops below 1.5"

Choose the number data and parity slices depending on required level of protection and the cluster configuration. The number of storage targets must be greater than sum of the number of data and parity slices. If the cluster uses only replication (by setting `objsize_limit` to a very high value), the number of storage targets must exceed the number of parity slices.

Notes:

- Every data and parity slice is stored on a separate storage target. To reconstruct a damaged object, AIStore requires at least `ec.data_slices` slices in total out of data and parity sets
- Small objects are replicated `ec.parity_slices` times to have the same level of data protection that big objects do
- Increasing the number of parity slices improves data protection level, but it may hit performance: doubling the number of slices approximately increases the time to encode the object by a factor of two

Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops","value":{"lru":{"lowwm":1,"highwm":100,"atime_cache_max":1,"dont_evict_time":"990m","capacity_upd_time":"90m","enabled":true}, "ec": {"enabled": true, "data": 4, "parity": 2}}}' 'http://G/v1/buckets/<bucket-name>'
```

To change only one EC property(e.g, enable or disable EC for a bucket) without touching other bucket properties, use the single set property API. Example of disabling EC:

```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "name": "ec.enabled", "value": false}' 'http://G/v1/buckets/<bucket-name>'
```

### Limitations

In version 2.1, once a bucket is configured for EC, it'll stay erasure coded for its entire lifetime - there is currently no supported way to change this once-applied configuration to a different (N, K) schema, disable EC, and/or remove redundant EC-generated content.

Secondly, only ais buckets are currently supported. Both limitations will be removed in the subsequent releases.

## N-way mirror
Yet another supported storage service is n-way mirroring providing for bucket-level data redundancy and data protection. The service makes sure that each object in a given distributed (local or Cloud) bucket has exactly **n** object replicas, where n is an arbitrary user-defined integer greater or equal 1.

In other words, AIS n-way mirroring is intended to withstand loss of disks, not storage nodes (aka AIS targets).

> For the latter, please consider using #erasure-coding and/or any of the alternative backup/restore mechanisms.

The service ensures is that for any given object there will be *no two replicas* sharing the same local disk.

> Unlike [erasure coding](#erasure-coding) that takes care of distributing redundant content across *different* clustered nodes, local mirror is, as the name implies, local. When a bucket is [configured as a mirror](/ais/setup/config.sh), objects placed into this bucket get locally replicated and the replicas are stored in local filesystems.

> As aside, note that AIS storage targets can be deployed to utilize Linux LVMs that provide a variety of RAID/mirror schemas.

The following example configures buckets a, b, and c to store n = 1, 2, and 3 object replicas, respectively:

```shell
curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "makencopies", "value":1}' 'http://G/v1/buckets/a'
curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "makencopies", "value":2}' 'http://G/v1/buckets/b'
curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "makencopies", "value":3}' 'http://G/v1/buckets/c'
```

The operations (above) are in fact [extended actions](xaction.md) that run asynchronously. Both Cloud and ais buckets are supported. You can monitor completion of those operations via generic [xaction API](xaction.md).

Subsequently, all PUTs into an n-way configured bucket also generate **n** copies for all newly created objects. Which also goes to say that the ("makencopies") operation, in addition to creating or destroying replicas of existing objects will also automatically re-enable(if n > 1) or disable (if n == 1) mirroring as far as subsequent PUTs are concerned.

Note again that number of local replicas is defined on a per-bucket basis.

### Read load balancing
With respect to n-way mirrors, the usual pros-and-cons consideration boils down to (the amount of) utilized space, on the other hand, versus data protection and load balancing, on the other.

Since object replicas are end-to-end protected by [checksums](#checksumming) all of them and any one in particular can be used interchangeably to satisfy a GET request thus providing for multiple possible choices of local filesystems and, ultimately, local drives. Given n > 1, AIS will utilize the least loaded drive(s).

### More examples
The following sequence creates a bucket named `abc`, PUTs an object into it and then converts it into a 3-way mirror:

```shell
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "createlb"}' 'http://G/v1/buckets/abc'
$ curl -L -X PUT 'http://G/v1/objects/abc/obj1' -T /tmp/obj1
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "makencopies", "value":3}' 'http://G/v1/buckets/abc'
```

The next command will redefine the `abc` bucket created in the previous example as a 2-way mirror - all objects that were previously stored in three replicas will now have only two (replicas):

```shell
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "makencopies", "value":2}' 'http://G/v1/buckets/abc'
```
