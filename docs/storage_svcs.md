## Table of Contents
- [Storage Services](#storage-services)
    - [Checksumming](#checksumming)
    - [LRU](#lru)
    - [Erasure coding](#erasure-coding)
    - [Local mirroring and load balancing](#local-mirroring-and-load-balancing)

## Storage Services

By default, buckets inherit [global configuration](../ais/setup/config.sh). However, several distinct sections of this global configuration can be overridden at startup or at runtime on a per bucket basis. The list includes checksumming, LRU, erasure coding, and local mirroring - please see the following sections for details.

### Checksumming

Checksumming on bucket level is configured by setting bucket properties:

* `cksum_config.checksum`: `"none"`,`"xxhash"` or `"inherit"` configure hashing type. Value
`"inherit"` indicates that the global checksumming configuration should be used.
* `cksum_config.validate_checksum_cold_get`: `true` or `false` indicate
whether to perform checksum validation during cold GET.
* `cksum_config.validate_checksum_warm_get`: `true` or `false` indicate
whether to perform checksum validation during warm GET.
* `cksum_config.enable_read_range_checksum`: `true` or `false` indicate whether to perform checksum validation during byte serving.

Value for the `checksum` field (see above) *must* be provided *every* time the bucket properties are updated, otherwise the request will be rejected.

Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "value": {"cksum_config": {"checksum": "xxhash", "validate_checksum_cold_get": true, "validate_checksum_warm_get": false, "enable_read_range_checksum": false}}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

### LRU

Overriding the global configuration can be achieved by specifying the fields of the `LRUProps` instance of the `lruconfig` struct that encompasses all LRU configuration fields.

* `lru_props.lowwm`: integer in the range [0, 100], representing the capacity usage low watermark
* `lru_props.highwm`: integer in the range [0, 100], representing the capacity usage high watermark
* `lru_props.atime_cache_max`: positive integer representing the maximum number of entries
* `lru_props.dont_evict_time`: string that indicates eviction-free period [atime, atime + dont]
* `lru_props.capacity_upd_time`: string indicating the minimum time to update capacity
* `lru_props.lru_enabled`: bool that determines whether LRU is run or not; only runs when true

**NOTE**: In setting bucket properties for LRU, any field that is not explicitly specified is defaulted to the data type's zero value.
Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops","value":{"cksum_config":{"checksum":"none","validate_checksum_cold_get":true,"validate_checksum_warm_get":true,"enable_read_range_checksum":true},"lru_props":{"lowwm":1,"highwm":100,"atime_cache_max":1,"dont_evict_time":"990m","capacity_upd_time":"90m","lru_enabled":true}}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

To revert a bucket's entire configuration back to use global parameters, use `"action":"resetprops"` to the same PUT endpoint as above as such:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"resetprops"}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

### Erasure coding

AIStore provides data protection that comes in several flavors: [end-to-end checksumming](#checksumming), [Local mirroring](#local-mirroring-and-load-balancing), replication (for *small* objects), and erasure coding.

Erasure coding, or EC, is a well-known storage technique that protects user data by dividing it into N fragments or slices, computing K redundant (parity) slices, and then storing the resulting (N+K) slices on (N+K) storage servers - one slice per target server.

EC schemas are flexible and user-configurable: users can select the N and the K (above), thus ensurng that user data remains available even if the cluster loses **any** (emphasis on the **any**) of its K servers.

* `ec_config.enabled`: bool - enables or disabled data protection the bucket
* `ec_config.data_slices`: integer in the range [2, 100], representing the number of fragments the object is broken into
* `ec_config.parity_slices`: integer in the range [2, 32], representing the number of redundant fragments to provide prtection from failures. The value defines the maximum number of storage targets a cluster can lose but it is still able to restore the original object
* `ec_config.objsize_limit`: integer indicating the minimum size of an object that is erasure encoded. Smaller objects are just replicated. The field can be 0 - in this case the default value is used (as of version 1.3 it is 256KiB)

Choose the number data and parity slices depending on required level of protection and the cluster configuration. The number of storage targets must be greater than sum of the number of data and parity slices. If the cluster uses only replication (by setting objsize_limit to a very high value), the number of storage targets must exceed the number of parity slices.

Notes:

- Every data and parity slice is stored on a separate storage target. To reconstruct a damaged object, AIStore requires at least `ec_config.data_slices` slices in total out of data and parity sets
- Small objects are replicated `ec_config.parity_slices` times to have the same level of data protection that big objects do
- Increasing the number of parity slices improves data protection level, but it may hit performance: doubling the number of slices approximately increases the time to encode the object by a factor of two

Example of setting bucket properties:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops","value":{"lru_props":{"lowwm":1,"highwm":100,"atime_cache_max":1,"dont_evict_time":"990m","capacity_upd_time":"90m","lru_enabled":true}, "ec_config": {"enabled": true, "data": 4, "parity": 2}}}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

To change ony one EC property(e.g, enable or disable EC for a bucket) without touching other bucket properties, use the sinlge set property API. Example of disabling EC:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "name": "ec_config-enabled", "value":"false"}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

#### Limitations

In the version 2.0, once a bucket is configured for EC, it'll stay erasure coded for its entire lifetime - there is currently no supported way to change this once-applied configuration to a different (N, K) schema, disable EC, and/or remove redundant EC-generated content.

Secondly, only local buckets are currently supported. Both limitations will be removed in the subsequent releases.

### Local mirroring and load balancing

Unlike erasure coding (above) that takes special care of distributing redundant content across *different* clustered nodes, local mirror is, as the the name implies, local. When a bucket is [configured as a mirror](../ais/setup/config.sh), objects placed into this bucket get locally replicated and the replicas are stored on a local filesystems that are different from those that store the original. In other words, a mirrorred bucket will survive a loss of any (one) local drive.

>> The last statement is especially true when local filesystems are non-redundant. As a side, note that AIS storage targets can be deployed to utilize Linux LVMs that (themselves) provide a variety of RAID/mirror schemas.

Further, as of v2.0 the capability entails:

* at PUT time: asynchronously generate local replicas while trying to minimize the interference with user workloads
* at GET time: given 2 (two) choices, select the least loaded drive or drives that store the requested object

Finally, all accumulated redundant content can be (asynchronously) destroyed at any time via specific [extended action](../docs/xaction.md) called [erasecopies](../cmn/api.go). And as always, the same can be achieved via the following `curl`:

```shell
$ curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"erasecopies"}' http://localhost:8080/v1/buckets/abc
```

To change ony one mirroring property without touching other bucket properties, use the sinlge set property API. Example of disabling mirroring:
```shell
$ curl -i -X PUT -H 'Content-Type: application/json' -d '{"action":"setprops", "name": "mirror-mirror_enabled", "value":"false"}' 'http://localhost:8080/v1/buckets/<bucket-name>'
```

>> The `curl` example above uses gateway's URL `localhost:8080` and bucket named `abc` as an example...
