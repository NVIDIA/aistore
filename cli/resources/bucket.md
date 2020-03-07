---
layout: post
title: BUCKET
permalink: cli/resources/bucket
redirect_from:
- cli/resources/bucket.md/
---

## Bucket

The CLI allows users to interact with [buckets](../../docs/bucket.md) in the AIS cluster.

### Create

`ais create bucket BUCKET_NAME [BUCKET_NAME...]`

Create an ais bucket or buckets.

#### Examples

1) Correct local buckets creation

```console
# Create buckets mybucketA and mybucketB, both with AIS provider (mybucketB provider is implicit) 
$ ais create bucket ais://mybucketA mybucketB
```

2) Incorrect buckets creation

```console
$ ais create bucket cloud://mybucket
Cloud buckets not allowed (cloud://mybucket).
```

### Delete

`ais rm bucket BUCKET [BUCKET...]`

Delete an ais bucket or buckets.

1) Correct local buckets removal

```console
# Removes local buckets mybucketA and mybucketB
$ ais rm bucket ais://mybucketA mybucketB
```

2) Incorrect buckets removal

```console
$ ais rm bucket cloud://mybucket
Cloud buckets not allowed (cloud://mybucket).
```


### List bucket names

`ais ls`

List all bucket names.

`ais ls cloud://` or `ais ls ais://`

List all bucket names for the specific provider.

#### Flags for listing bucket names

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Pattern for matching bucket names | `""` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

### List object names

#### With provider auto-detection

`ais ls BUCKET_NAME`

List object names in the bucket `BUCKET_NAME`. Cloud provider is auto-detected.

#### From the specific provider

`ais ls ais://BUCKET_NAME`

List objects in the AIS bucket `BUCKET_NAME`.

`ais ls cloud://BUCKET_NAME`

List objects in the cloud bucket `BUCKET_NAME`.

#### Flags for listing object names

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Pattern for matching object names | `""` |
| `--template` | `string` | Template for matching object names | `""` |
| `--prefix` | `string` | Prefix for matching object names | `""` |
| `--fast` | `bool` | Use fast API to list all object names | `false` |
| `--paged` | `bool` | Fetch and print objects page by page (ignored in fast mode) | `false` |
| `--max-pages` | `int` | Max. number of pages to list | `0` |
| `--page-size` | `int` | Max. number of object names per page | `1000` |
| `--props` | `string` | Comma-separated properties to return with object names (ignored in fast mode) | `"size,version"`
| `--limit` | `int` | Max. number of object names to list (ignored in fast mode) | `0` |
| `--show-unmatched` | `bool` | List objects unmatched by regex and template as well, after the matched ones | `false` |
| `--all-items` | `bool` | Show all items, including all, duplicated, etc. (ignored in fast mode) | `false` |
| `--marker` | `string` | Start listing objects starting from the object that follows the marker alphabetically (ignored in fast mode) | `""` |
| `--no-headers` | `bool` | Display tables without headers | `false` |
| `--cached` | `bool` | For a cloud bucket, shows only objects that have already been downloaded and are cached on local drives (ignored for ais buckets) | `false` |

#### Examples

```console
$ ais ls ais://mybucket # list local bucket mybucket
```

```console
$ ais ls cloud://mybucket # list cloud bucket mybucket
```

### Evict

`ais evict BUCKET_NAME`

Evict a cloud bucket. It also resets the properties of the bucket (if changed).

#### Examples

```console
$ ais evict mybucket # evict cloud bucket mybucket
```

```console
$ ais evict ais://mybucket # FAIL
Evict command doesn't support local buckets.
```
### Rename

`ais rename bucket BUCKET_NAME NEW_NAME`

Rename an ais bucket.

#### Examples

```console
$ ais rename bucket ais://mybucket mynewbucket # rename local bucket `mybucket` to local bucket `mynewbucket`
```

```console
$ ais rename bucket cloud://mycloudbucket cloud://mynewbucket # FAIL
Renaming of cloud buckets not supported
```

### Copy

`ais cp bucket BUCKET_NAME NEW_NAME`

Copy an existing ais bucket to a new ais bucket.

#### Examples

```console
$ ais cp bucket ais://mybucket mynewbucket # copy local bucket `mybucket` to local bucket `mynewbucket`
```

```console
$ ais cp bucket cloud://mycloudbucket cloud://mynewbucket # FAIL
Copying of cloud buckets not supported
```

### Summary

`ais show bucket [BUCKET_NAME]`

Show aggregated information about objects in the bucket `BUCKET_NAME`.
If `BUCKET_NAME` is omitted, shows information about all buckets.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--fast` | `bool` | Enforce using faster methods to find out the buckets' details. The output may not be accurate. | `false`

### Make N copies

`ais set-copies BUCKET_NAME --copies <value>`

Start an extended action to bring a given bucket to a certain redundancy level (num copies). Read more about this feature [here](../../docs/storage_svcs.md#n-way-mirror).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--copies` | `int` | Number of copies | `1` |

### Make all objects erasure coded

`ais ec-encode BUCKET_NAME`

Start an extended action that enables data protection for all objects of a given bucket. Erasure coding must be set up for the bucket prior to running `ec-encode` extended action. Read more about this feature [here](../../docs/storage_svcs.md#erasure-coding).

### Show bucket props

`ais show props BUCKET_NAME`

List [properties](../../docs/bucket.md#properties-and-options) of the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output in JSON format | `false` |

### Set bucket props

`ais set props BUCKET_NAME KEY=VALUE [KEY=VALUE...]`

Set bucket properties. For the available options, see [bucket-properties](../../docs/bucket.md#properties-and-options).
If `--reset` flag is set, arguments are ignored and bucket properties are reset to original state.
If `--jsonspec` option is used, **all** properties of the bucket are set based on the values in the JSON object.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--jsonspec` | `string` | Bucket properties in a JSON format | `""` |
| `--reset` | `bool` | Reset bucket properties to original state | `false` |

When `--jsonspec` is not used, some properties support user-friendly aliases:

| Property | Value alias | Description |
| --- | --- | --- |
| aattrs | ro | Disables bucket modifications: denies PUT, DELETE, and ColdGET requests |
| aattrs | rw | Enables bucket modifications: allows PUT, DELETE, and ColdGET requests |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais set props mybucket 'mirror.enabled=true' 'mirror.copies=2'` | Sets the `mirror.enabled` and `mirror.copies` properties to `true` and `2` respectively, for the bucket `mybucket` |
| `ais set props mybucket 'aattrs=ro'` | Sets read-only access to the bucket `mybucket`. All PUT and DELETE requests will fail |
| `ais set props --reset mybucket` | Resets properties for the bucket `mybucket` |


Setting **all** bucket attributes based on the provided JSON specification

```console
$ ais set props mybucket --jsonspec '{
    "cloud_provider": "ais",
    "versioning": {
      "enabled": true,
      "validate_warm_get": false
    },
    "tiering": {},
    "cksum": {
      "type": "xxhash",
      "validate_cold_get": true,
      "validate_warm_get": false,
      "validate_obj_move": false,
      "enable_read_range": false
    },
    "lru": {
      "lowwm": 20,
      "highwm": 80,
      "out_of_space": 90,
      "dont_evict_time": "20m",
      "capacity_upd_time": "1m",
      "enabled": true
    },
    "mirror": {
      "copies": 0,
      "burst_buffer": 0,
      "util_thresh": 0,
      "optimize_put": false,
      "enabled": false
    },
    "ec": {
        "objsize_limit": 256000,
        "data_slices": 2,
        "parity_slices": 2,
        "enabled": true
    },
    "aattrs": 255
}'
```

```console
$ ais show props mybucket
Property	Value
Provider	ais
Access		GET,PUT,DELETE,HEAD,ColdGET
Checksum	xxhash (validation: ColdGET=yes, WarmGET,ObjectMove,ReadRange=no)
Mirror		Disabled
EC		2:2 (250KiB)
LRU		Watermarks: 20/80, do not evict time: 20m
Versioning	Enabled | Validate on WarmGET: no
Tiering		Disabled
```

If not all properties are mentioned in the JSON, the missing ones are set to zero values (empty / `false` / `nil`):

```console
$ ais set props mybucket --jsonspec '{
  "mirror": {
    "enabled": true
  },
  "versioning": {
    "enabled": true,
    "validate_warm_get": true
  }
}'
```

```console
$ ais show props mybucket
Property        Value
Provider        ais
Access          No access
Checksum        xxhash (validation: ColdGET=yes, WarmGET,ObjectMove,ReadRange=no)
Mirror          2 Copies
EC              Disabled
LRU             Disabled
Versioning      Enabled | Validate on WarmGET: yes
Tiering         Disabled
```

To see how setting zero values affect properties, run:  `ais set props mybucket --jsonspec '{}'`
