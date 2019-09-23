## Bucket

The CLI allows users to interact with [buckets](../../docs/bucket.md) in the AIS cluster.

### Create

`ais create bucket BUCKET_NAME [BUCKET_NAME...]`

Creates an ais bucket or buckets.


### Delete

`ais rm bucket BUCKET [BUCKET...]`

Deletes an ais bucket or buckets.

### List bucket names

`ais ls bucket [BUCKET_PROVIDER]`

Lists bucket names. If the `BUCKET_PROVIDER` argument is given, only bucket names
from the specified provider are listed.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Pattern for bucket name matching | `""` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

### Evict

`ais evict BUCKET_NAME/`

Evicts a cloud bucket. It also resets the properties of the bucket (if changed).

### Rename

`ais rename bucket BUCKET_NAME NEW_NAME`

Renames an ais bucket.

### Copy

`ais cp bucket BUCKET_NAME NEW_NAME`

Copies an existing ais bucket to a new ais bucket.

### Summary

`ais show bucket [BUCKET_NAME]`

Shows aggregated information about objects in the bucket `BUCKET_NAME`.
If `BUCKET_NAME` is omitted, shows informations about all buckets.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |

### Make N copies

`ais set-copies BUCKET_NAME --copies <value>`

Starts an extended action to bring a given bucket to a certain redundancy level (num copies). Read more about this feature [here](../../docs/storage_svcs.md#n-way-mirror).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--copies` | `int` | number of copies | `1` |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |

### List bucket props

`ais ls props BUCKET_NAME`

Lists [properties](../../docs/bucket.md#properties-and-options) of the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--json` | `bool` | Output in JSON format | `false` |

### Set bucket props

`ais set props BUCKET_NAME KEY=VALUE [KEY=VALUE...]`

Sets bucket properties. For the available options, see [bucket-properties](../../docs/bucket.md#properties-and-options).
If `--reset` flag is set, arguments are ignored and bucket properties are reset to original state.
If `--jsonspec` option is used, **all** properties of the bucket are set based on the values in the JSON object.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider)|
| `--jsonspec` | `string` | bucket properties in a JSON format | `` |
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
```bash
ais set props mybucket --jsonspec '{
    "cloud_provider": "ais",
    "versioning": {
      "type": "own",
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
      "ais_buckets": false,
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
> After which `ais ls props mybucket` results in:
```
ais bucket props list mybucket
Property	Value
Provider	ais
Access		GET,PUT,DELETE,HEAD,ColdGET
Checksum	xxhash (validation: ColdGET=yes, WarmGET,ObjectMove,ReadRange=no)
Mirror		Disabled
EC		2:2 (250KiB)
LRU		Watermarks: 20/80, do not evict time: 20m
Versioning	(validation: WarmGET=no)
Tiering		Disabled

```

If not all properties are mentioned in the JSON, the missing ones are set to zero values (empty / `false` / `nil`):
```bash
ais set props mybucket --jsonspec '{
    "cloud_provider": "ais",
    "versioning": {
      "type": "own",
      "enabled": true,
      "validate_warm_get": true
    }
}'
```
> After which `ais ls props mybucket` results in:
```
Property        Value
Provider        ais
Access          No access
Checksum        xxhash (validation: ColdGET=yes, WarmGET,ObjectMove,ReadRange=no)
Mirror          Disabled
EC              Disabled
LRU             Disabled
Versioning      (validation: WarmGET=yes)
Tiering         Disabled
```

To see how setting zero values affect properties, run:  `ais set props mybucket --jsonspec '{}'`

### Default `BUCKET_NAME` argument value
If `AIS_BUCKET` environment variable is set, its value is used as the default value for `BUCKET_NAME` arguments
in above-mentioned commands if `BUCKET_NAME` argument is not given by the user. For example, the following pairs of commands have the same effect:

| With `AIS_BUCKET` | Without `AIS_BUCKET` |
| --- | --- |
| `AIS_BUCKET=mybucket ais create bucket` | `ais create bucket mybucket` |
| `AIS_BUCKET=mybucket ais rename bucket mybucket1` | `ais rename bucket mybucket mybucket1` |
| `AIS_BUCKET=mybucket ais set props 'aattrs=ro'` | `ais set props mybucket 'aattrs=ro'` |
