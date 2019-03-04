## Table of Contents
- [Storage Services](#storage-services)
    - [Checksumming](#checksumming)
    - [LRU](#lru)
    - [Erasure coding](#erasure-coding)
    - [Local mirroring and load balancing](#local-mirroring-and-load-balancing)

## Storage Services

By default, buckets inherit [global configuration](/ais/setup/config.sh). However, several distinct sections of this global configuration can be overridden at startup or at runtime on a per bucket basis. The list includes checksumming, LRU, erasure coding, and local mirroring - please see the following sections for details.

### Checksumming

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
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"cksum": {"type": "xxhash", "validate_cold_get": true, "validate_warm_get": false, "enable_read_range": false}}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

### LRU

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
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops","value":{"cksum":{"type":"none","validate_cold_get":true,"validate_warm_get":true,"enable_read_range":true},"lru":{"lowwm":1,"highwm":100,"atime_cache_max":1,"dont_evict_time":"990m","capacity_upd_time":"90m","enabled":true}}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

To revert a bucket's entire configuration back to use global parameters, use `"action":"resetprops"` to the same PUT endpoint as above as such:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"resetprops"}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```
#### LRU for local buckets

LRU eviction, as of version 2.0, is by default only enabled for cloud buckets. To enable for local buckets, set `lru.local_buckets` to true in [config.sh](/ais/setup/config.sh) before deploying AIS. Note that this is for advanced usage only, since this causes automatic deletion of objects in local buckets, and therefore can cause data to be gone forever if not backed up outside of AIS.

### Erasure coding

AIStore provides data protection that comes in several flavors: [end-to-end checksumming](#checksumming), [Local mirroring](#local-mirroring-and-load-balancing), replication (for *small* objects), and erasure coding.

Erasure coding, or EC, is a well-known storage technique that protects user data by dividing it into N fragments or slices, computing K redundant (parity) slices, and then storing the resulting (N+K) slices on (N+K) storage servers - one slice per target server.

EC schemas are flexible and user-configurable: users can select the N and the K (above), thus ensuring that user data remains available even if the cluster loses **any** (emphasis on the **any**) of its K servers.

* `ec.enabled`: bool - enables or disabled data protection the bucket
* `ec.data_slices`: integer in the range [2, 100], representing the number of fragments the object is broken into
* `ec.parity_slices`: integer in the range [2, 32], representing the number of redundant fragments to provide protection from failures. The value defines the maximum number of storage targets a cluster can lose but it is still able to restore the original object
* `ec.objsize_limit`: integer indicating the minimum size of an object that is erasure encoded. Smaller objects are just replicated. The field can be 0 - in this case the default value is used (as of version 1.3 it is 256KiB)

Choose the number data and parity slices depending on required level of protection and the cluster configuration. The number of storage targets must be greater than sum of the number of data and parity slices. If the cluster uses only replication (by setting `objsize_limit` to a very high value), the number of storage targets must exceed the number of parity slices.

Notes:

- Every data and parity slice is stored on a separate storage target. To reconstruct a damaged object, AIStore requires at least `ec.data_slices` slices in total out of data and parity sets
- Small objects are replicated `ec.parity_slices` times to have the same level of data protection that big objects do
- Increasing the number of parity slices improves data protection level, but it may hit performance: doubling the number of slices approximately increases the time to encode the object by a factor of two

Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops","value":{"lru":{"lowwm":1,"highwm":100,"atime_cache_max":1,"dont_evict_time":"990m","capacity_upd_time":"90m","enabled":true}, "ec": {"enabled": true, "data": 4, "parity": 2}}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

To change ony one EC property(e.g, enable or disable EC for a bucket) without touching other bucket properties, use the sinlge set property API. Example of disabling EC:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "name": "ec.enabled", "value": false}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

#### Limitations

In the version 2.0, once a bucket is configured for EC, it'll stay erasure coded for its entire lifetime - there is currently no supported way to change this once-applied configuration to a different (N, K) schema, disable EC, and/or remove redundant EC-generated content.

Secondly, only local buckets are currently supported. Both limitations will be removed in the subsequent releases.

### Local mirroring and load balancing

Unlike erasure coding (above) that takes special care of distributing redundant content across *different* clustered nodes, local mirror is, as the the name implies, local. When a bucket is [configured as a mirror](/ais/setup/config.sh), objects placed into this bucket get locally replicated and the replicas are stored on a local filesystems that are different from those that store the original. In other words, a mirrored bucket will survive a loss of any (one) local drive.

>> The last statement is especially true when local filesystems are non-redundant. As a side, note that AIS storage targets can be deployed to utilize Linux LVMs that (themselves) provide a variety of RAID/mirror schemas.

Further, as of v2.0 the capability entails:

* at PUT time: asynchronously generate local replicas while trying to minimize the interference with user workloads
* at GET time: given 2 (two) choices, select the least loaded drive or drives that store the requested object

Finally, all accumulated redundant content can be (asynchronously) destroyed at any time via specific [extended action](/docs/xaction.md) called [erasecopies](/cmn/api.go). And as always, the same can be achieved via the following `curl`:

```shell
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"erasecopies"}' http://localhost:8080/v1/buckets/abc
```

To change ony one mirroring property without touching other bucket properties, use the sinlge set property API. Example of disabling mirroring:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "name": "mirror.enabled", "value": false}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

>> The `curl` example above uses gateway's URL `localhost:8080` and bucket named `abc` as an example...
