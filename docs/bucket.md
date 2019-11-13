## Table of Contents
- [Bucket](#bucket)
  - [Bucket Provider](#bucket-provider)
- [AIS Bucket](#ais-bucket)
  - [Curl examples: create, rename and, destroy ais bucket](#curl-examples-create-rename-and-destroy-ais-bucket)
- [Cloud Bucket](#cloud-bucket)
  - [Prefetch/Evict Objects](#prefetchevict-objects)
  - [Evict Cloud Bucket](#evict-cloud-bucket)
- [Bucket Access Attributes](#bucket-access-attributes)
- [List Bucket](#list-bucket)
  - [Properties and Options](#properties-and-options)
  - [Curl example: listing ais and Cloud buckets](#curl-example-listing-ais-and-cloud-buckets)
  - [CLI examples: listing and setting bucket properties](#cli-examples-listing-and-setting-bucket-properties)
- [Recover Buckets](#recover-buckets)
  - [Example: recovering buckets](#example-recovering-buckets)

## Bucket

As one would normally expect from an object store, AIS uses the bucket abstraction.

Buckets in AIS serve as the basic container in which objects are stored, similar to the buckets in [Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/dev/UsingBucket.html) and [Google Cloud (GCP)](https://cloud.google.com/storage/docs/key-terms#buckets).

AIS supports two kinds of buckets: **ais buckets** and **3rd party Cloud-based buckets** (or simply **cloud buckets**).

All the [supported storage services](storage_svcs.md) equally apply to both kinds of buckets, with only a few exceptions. The following table summarizes them.

| Kind | Description | Supported Storage Services (as of v2.0) |
| --- | --- | --- |
| ais buckets | buckets that are **not** 3rd party Cloud-based. AIS buckets store user objects and support user-specified bucket properties (e.g., 3 copies). Unlike cloud buckets, ais buckets can be created through the [RESTful API](http_api.md). Similar to cloud buckets, ais buckets are distributed and balanced, content-wise, across the entire AIS cluster. | [Checksumming](storage_svcs.md#checksumming), [LRU (advanced usage)](storage_svcs.md#lru-for-local-buckets), [Erasure Coding](storage_svcs.md#erasure-coding), [Local Mirroring and Load Balancing](storage_svcs.md#local-mirroring-and-load-balancing) |
| cloud buckets | When AIS is deployed as [fast tier](/README.md#fast-tier), buckets in the cloud storage can be viewed and accessed through the [RESTful API](http_api.md) in AIS, in the exact same way as ais buckets. When this happens, AIS creates local instances of said buckets which then serves as a cache. These are referred to as **Cloud-based buckets** (or **cloud buckets** for short). | [Checksumming](storage_svcs.md#checksumming), [LRU](storage_svcs.md#lru), [Erasure Coding](storage_svcs.md#erasure-coding), [Local mirroring and load balancing](storage_svcs.md#local-mirroring-and-load-balancing) |

Cloud-based and ais buckets support the same API with minor exceptions. Cloud buckets can be *evicted* from AIS. AIS buckets are the only buckets that can be created, renamed, and deleted via the [RESTful API](http_api.md).

> Most of the examples below are `curl` based; it is possible, however, and often even preferable, to execute the same operations using [AIS CLI](../cli/README.md). In particular, for the commands that operate on buckets, please refer to [this CLI resource](../cli/resources/bucket.md).

### Bucket Provider

Any storage bucket handled by AIS may originate in a 3rd party Cloud, or be created (and subsequently filled-in) in the AIS itself. But what if there's a pair of buckets, a Cloud-based and, separately, an ais bucket, that happen to share the same name? To resolve the potential naming conflict, AIS introduces the concept of *bucket provider*.

> Bucket provider is realized as an optional parameter in the GET, PUT, DELETE and [Range/List](batch.md) operations with supported enumerated values: `local` and `ais` for ais buckets, and `cloud`, `aws`, `gcp` for cloud buckets.

For detailed documentation please refer [to the RESTful API reference and examples](http_api.md). Rest of this document serves to further explain features and concepts specific to storage buckets.

## AIS Bucket
AIS buckets are the AIStore-own distributed buckets that are not associated with any 3rd party Cloud.

The [RESTful API](docs/http_api.md) can be used to create, rename and, destroy ais buckets.

New ais buckets must be given a unique name that does not duplicate any existing ais or cloud bucket.

### Curl examples: create, rename and, destroy ais bucket

To create an ais bucket with the name 'myBucket', rename it to 'myBucket2' and delete it, run:

```shell
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "createlb"}' http://localhost:8080/v1/buckets/myBucket
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "renamelb",  "name": "myBucket2"}' http://localhost:8080/v1/buckets/myBucket
$ curl -X DELETE -L -H 'Content-Type: application/json' -d '{"action": "destroylb"}' http://localhost:8080/v1/buckets/myBucket2
```

## Cloud Bucket

Cloud buckets are existing buckets in the cloud storage when AIS is deployed as [fast tier](/README.md#fast-tier).

> By default, AIS does not keep track of the cloud buckets in its configuration map. However, if users modify the properties of the cloud bucket, AIS will then keep track.

### Prefetch/Evict Objects

Objects within cloud buckets are automatically fetched into storage targets when accessed through AIS, and are evicted based on the monitored capacity and configurable high/low watermarks when [LRU](storage_svcs.md#lru) is enabled.

The [RESTful API](docs/http_api.md) can be used to manually fetch a group of objects from the cloud bucket (called prefetch) into storage targets, or to remove them from AIS (called evict).

Objects are prefetched or evicted using [List/Range Operations](batch.md#listrange-operations).

For example, to use a [list operation](batch.md#list) to prefetch 'o1', 'o2', and, 'o3' from the cloud bucket `abc`, run:

```shell
curl -i -X POST -H 'Content-Type: application/json' -d '{"action":"prefetch", "value":{"objnames":["o1","o2","o3"], "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc
```

See the [List/Range Operation section](batch.md#listrange-operations) for the `"deadline": "10s"` and `"wait":true` parameters.

To use a [range operation](batch.md#range) to evict the 1000th to 2000th objects in the cloud bucket `abc` from AIS, which begin with the prefix `__tst/test-` and contain a 4 digit number with `22` in the middle, run:

```shell
curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action":"evictobjects", "value":{"prefix":"__tst/test-", "regex":"\\d22\\d", "range":"1000:2000", "deadline": "10s", "wait":true}}' http://localhost:8080/v1/buckets/abc
```

### Evict Cloud Bucket

Before a cloud bucket is accessed through AIS, the cluster has no awareness of the bucket.

Once there is a request to access the bucket, or a request to change the bucket's properties (see `set bucket props` in [REST API](http_api.md)), then the AIS cluster starts keeping track of the bucket.

In an evict bucket operation, AIS will remove all traces of the cloud bucket within the AIS cluster. This effectively resets the AIS cluster to the point before any requests to the bucket have been made. This does not affect the objects stored within the cloud bucket.

For example, to evict the `abc` cloud bucket from the AIS cluster, run:

```shell
curl -i -X DELETE -H 'Content-Type: application/json' -d '{"action": "evictcb"}' http://localhost:8080/v1/buckets/myS3bucket
```

## Bucket Access Attributes

Bucket access is controlled by a single 64-bit `aattrs` value in the [Bucket Properties structure](../cmn/api.go), whereby its bits have the following mapping as far as allowed (or denied) operations:

| Operation | Bit Mask |
| --- | --- |
| GET | 0x1 |
| HEAD | 0x2 |
| PUT | 0x4 |
| Cold GET | 0x8 |
| DELETE | 0x16 |

For instance, to make bucket `abc` read-only, execute the following [AIS CLI](../cli/README.md) command:

```shell
ais set props abc 'aattrs=ro'
```

The same expressed via `curl` will look as follows:

```shell
curl -i -X PUT 'http://localhost:8080/v1/buckets/abc/setprops?aattrs=18446744073709551587'
```

> 18446744073709551587 = 0xffffffffffffffe3 = 0xffffffffffffffff ^ (4|8|16)

## List Bucket

ListBucket API returns a page of object names and, optionally, their properties (including sizes, access time, checksums, and more), in addition to a token that servers as a cursor or a marker for the *next* page retrieval.

### Properties and options
The properties-and-options specifier must be a JSON-encoded structure, for instance '{"props": "size"}' (see examples). An empty structure '{}' results in getting just the names of the objects (from the specified bucket) with no other metadata.

| Property/Option | Description | Value |
| --- | --- | --- |
| props | The properties to return with object names | A comma-separated string containing any combination of: "checksum","size","atime","version","targetURL","copies","status". <sup id="a6">[6](#ft6)</sup> |
| time_format | The standard by which times should be formatted | Any of the following [golang time constants](http://golang.org/pkg/time/#pkg-constants): RFC822, Stamp, StampMilli, RFC822Z, RFC1123, RFC1123Z, RFC3339. The default is RFC822. |
| prefix | The prefix which all returned objects must have | For example, "my/directory/structure/" |
| pagemarker | The token identifying the next page to retrieve | Returned in the "nextpage" field from a call to ListBucket that does not retrieve all keys. When the last key is retrieved, NextPage will be the empty string |
| pagesize | The maximum number of object names returned in response | Default value is 1000. GCP and ais bucket support greater page sizes. AWS is unable to return more than [1000 objects in one page](https://docs.aws.amazon.com/AmazonS3/latest/API/RESTBucketGET.html) |
| fast | Perform fast traversal of bucket contents | If `true`, the list of objects is generated much faster but the result is less accurate and has a few limitations: only name of object is returned(props is ignored) and paging is unsupported as it always returns the entire bucket list(unless prefix is defined) |
| cached | Return only objects that are cached on local drives | For ais buckets the option is ignored. For cloud buckets, if `cached` is `true`, the cluster does not retrieve any data from the cloud, it reads only information from local drives |
| taskid | ID of the list bucket operation (string) | Listing a bucket is an asynchronous operation. First, a client should start the operation by sending `"0"` as `taskid` - `"0"` means initialize a new list operation. In response a proxy returns a `taskid` generated for the operation. Then the client should poll the operation status using the same JSON-encoded structure but with `taskid` set to the received value. If the operation is still in progress the proxy returns status code 202(Accepted) and empty body. If the operation is completed, it returns 200(OK) and the list of objects. The proxy can return status 410(Gone) indicating that the operation restarted and got a new ID. In this case the client should read new operation ID from the response body |

The full list of bucket properties are:

| Bucket Property | JSON | Description | Fields |
| --- | --- | --- | --- |
| CloudProvider | cloud_provider | CloudProvider can be "aws", "gcp" (clouds) - or "ais" (local) | `"cloud_provider": "aws" \| "gcp" \| "ais"` |
| NextTierURL | next_tier_url | NextTierURL is an absolute URI corresponding to the primary proxy of the next tier configured for the bucket specified | `"next_tier_url": "http://G-other"` |
| ReadPolicy | read_policy | ReadPolicy determines if a read will be from cloud or next tier specified by NextTierURL. Default: "next_tier" |   `"read_policy": "next_tier" \| "cloud"` |
| WritePolicy | write_policy | WritePolicy determines if a write will be to cloud or next tier specified by NextTierURL. Default: "cloud" | `"write_policy": "next_tier" \| "cloud"` |
| Cksum | cksum | Configuration for [Checksum](docs/checksum.md). `validate_cold_get` determines whether or not the checksum of received object is checked after downloading it from the cloud or next tier. `validate_warm_get`: determines if the object's version (if in Cloud-based bucket) and checksum are checked. If either value fail to match, the object is removed from local storage. `validate_cluster_migration` determines if the migrated objects across single cluster should have their checksum validated. `enable_read_range` returns the read range checksum otherwise return the entire object checksum.  | `"cksum": { "type": "none" \| "xxhash" \| "md5" \| "inherit", "validate_cold_get": bool,  "validate_warm_get": bool,  "validate_cluster_migration": bool, "enable_read_range": bool }` |
| LRU | lru | Configuration for [LRU](docs/storage_svcs.md#lru). `lowwm` and `highwm` is the used capacity low-watermark and high-watermark (% of total local storage capacity) respectively. `out_of_space` if exceeded, the target starts failing new PUTs and keeps failing them until its local used-cap gets back below `highwm`. `atime_cache_max` represents the maximum number of entries. `dont_evict_time` denotes the period of time during which eviction of an object is forbidden [atime, atime + `dont_evict_time`]. `capacity_upd_time` denotes the frequency at which AIStore updates local capacity utilization. `enabled` LRU will only run when set to true. | `"lru": { "lowwm": int64, "highwm": int64, "out_of_space": int64, "atime_cache_max": int64, "dont_evict_time": "120m", "capacity_upd_time": "10m", "enabled": bool }` |
| Mirror | mirror | Configuration for [Mirroring](docs/storage_svcs.md#local-mirroring-and-load-balancing). `copies` represents the number of local copies. `burst_buffer` represents channel buffer size.  `util_thresh` represents the threshold when utilizations are considered equivalent. `optimize_put` represents the optimization objective. `enabled` will only generate local copies when set to true. | `"mirror": { "copies": int64, "burst_buffer": int64, "util_thresh": int64, "optimize_put": bool, "enabled": bool }` |
| EC | ec | Configuration for [erasure coding](docs/storage_svcs.md#erasure-coding). `objsize_limit` is the limit in which objects below this size are replicated instead of EC'ed. `data_slices` represents the number of data slices. `parity_slices` represents the number of parity slices/replicas. `enabled` represents if EC is enabled. | `"ec": { "objsize_limit": int64, "data_slices": int, "parity_slices": int, "enabled": bool }` |


`SetBucketProps` allows the following configurations to be changed:

| Property | Type | Description |
| --- | --- | --- |
| `ec.enabled` | bool | enables EC on the bucket |
| `ec.data_slices` | int | number of data slices for EC |
| `ec.parity_slices` | int | number of parity slices for EC |
| `ec.objsize_limit` | int | size limit in which objects below this size are replicated instead of EC'ed |
| `ec.compression` | string | LZ4 compression parameters used when EC sends its fragments and replicas over network |
| `mirror.enabled` | bool | enable local mirroring |
| `mirror.copies` | int | number of local copies |
| `mirror.util_thresh` | int | threshold when utilizations are considered equivalent |



 <a name="ft6">6</a>: The objects that exist in the Cloud but are not present in the AIStore cache will have their atime property empty (""). The atime (access time) property is supported for the objects that are present in the AIStore cache. [↩](#a6)

### Curl example: listing ais and Cloud buckets

Example of listing objects in the smoke/ subdirectory of a given bucket called 'myBucket', the result must include object respective sizes and checksums.

Bucket list API is asynchronous, so it cannot be executed as one cURL command. The first request starts the task that enumerates objects in a background and returns the task ID to watch it:

```shell
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size, checksum", "prefix": "smoke/"}}' http://localhost:8080/v1/buckets/myBucket
5315610902768416055
```

Watch the task status, until it returns the list of objects. The requests is the same, except a field `taskid` that now contains the value returned by previous request(if `taskid` is zero or omitted, API starts a new task). If the task is still running, the request keeps responding with `taskid`. When the task completes, it returns the page content:

```shell
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size, checksum", "prefix": "smoke/", "taskid": "5315610902768416055"}}' http://localhost:8080/v1/buckets/myBucket
{
  "entries": [
    {
      "name": "zbIvfYiZlweQRORFBGGjOENsPigguamh",
      "size": 8388608,
      "checksum": "983bfada723b7fa1",
      "copies": 1,
      "flags": 64
    },
    {
      "name": "zqlvXFpMkEzZyezafixTXvHUHDHGzWGp",
      "size": 8388608,
      "checksum": "cb9a3c208f0bab01",
      "copies": 1,
      "flags": 64
    }
  ],
  "pagemarker": ""
}
```

The listing can be truncated: API returns at most 1000 objects per requests. If a bucket contains more objects, one has to requests the list page by page. Please, note the field `pagemarker`: empty `pagemarker` in response means that there are no more objects left, it is the last page. Non-empty `pagemarker` should be used to request the next page. Example of requesting two pages:

Start listing bucket from the first object (`taskid` is 0, and `pagemarker` is empty):

```shell

$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size, checksum", "prefix": "smoke/"}}' http://localhost:8080/v1/buckets/myBigBucket
7315610902768416055

$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size, checksum", "prefix": "smoke/", "taskid": "7315610902768416055"}}' http://localhost:8080/v1/buckets/myBigBucket
{
  "entries": [
    {
      "name": "zbIvfYiZlweQRORFBGGjOENsPigguamh",
      "size": 8388608,
	  # many lines skipped
      "copies": 1,
      "flags": 64
    }
  ],
  "pagemarker": "PLqOWWuiCXATlSkhTXbnXlFCNWVhCUGR"
}
```

Request the next page (`pagemarker` is copied from the previous response - it is the only difference from the first request; do not forget to set `taskid` to `0` or remove it):

```shell
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "listobjects", "value":{"props": "size, checksum", "prefix": "smoke/", "pagemarker": "PLqOWWuiCXATlSkhTXbnXlFCNWVhCUGR"}}' http://localhost:8080/v1/buckets/myBigBucket
4910019837373721
```

For many more examples, please refer to the [test sources](/ais/tests/) in the repository.

### CLI examples: listing and setting bucket properties

1. List bucket properties:

```shell
$ ais ls props mybucket
```

or, same via command argument (and without the environment variable):

```shell
$ ais ls props mybucket
```

or, the same to get output in a (raw) JSON form:

```shell
$ ais ls props mybucket --json
```

2. Enable erasure coding on a bucket:

```shell
$ ais set props mybucket ec.enabled=true
```

3. Enable object versioning and then list updated bucket properties:

```shell
$ ais set props mybucket ver.enabled=true
$ ais ls props mybucket
```

## Recover Buckets

After rebuilding a cluster and redeploying proxies, the primary proxy does not have information about buckets used in a previous session. But targets still contain the old data. The primary proxy can retrieve bucket metadata from all targets and then recreate the buckets.

Bucket recovering comes in two flavours: safe and forced. In safe mode the primary proxy requests bucket metadata from all targets in the cluster. If all the targets have the same metadata version, the primary applies received metadata and then synchronize the new information across the cluster. Otherwise, API returns an error. When force mode is enabled, the primary does not require all the targets to have the same version. The primary chooses the metadata with highest version and proceeds with it.

### Example: recovering buckets

To recover buckets *safely*, run:

```shell
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "recoverbck"}' http://localhost:8080/v1/buckets
```

To force recovering buckets:

```shell
$ curl -X POST -L -H 'Content-Type: application/json' -d '{"action": "recoverbck"}' http://localhost:8080/v1/buckets?force=true
```
