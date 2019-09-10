## Bucket

The CLI allows users to interact with [buckets](../../docs/bucket.md) in the AIS cluster.

## Command List

### create

`ais bucket create BUCKET [BUCKET...]`

Creates an ais bucket or buckets.


### destroy

`ais bucket destroy BUCKET [BUCKET...]`

Destroys an ais bucket or buckets.

### list

`ais bucket ls`

Returns the names of the buckets.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | string | pattern for bucket matching | `""` |
| `--provider` | [Provider](../README.md#enums) | returns `local` or `cloud` buckets. If empty, returns all bucket names. | `""` or [default](../README.md#bucket-provider) |

### evict

`ais bucket evict BUCKET`

Evicts a cloud bucket. It also resets the properties of the bucket (if changed).

### rename

`ais bucket rename BUCKET NEW_BUCKET`

Renames an ais bucket.

### copy

`ais bucket cp BUCKET NEW_BUCKET`

Copies an existing ais bucket to a new ais bucket.

### objects

`ais bucket objects BUCKET`

Lists all the objects along with some of the objects' properties. For the full list of properties, see [here](../../docs/bucket.md#list-bucket).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--props` | string | comma separated list of properties to return with object names | `size,version` |
| `--regex` | string | pattern for object matching | `""` |
| `--prefix` | string | prefix for object matching | `""` |
| `--template` | string | bash-style template for object matching | `""` |
| `--page-size` | string | maximum number of object names returned in response | `1000` (cloud), `65536` (local) |
| `--limit` | string | limit of object count | `0` (unlimited) |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--show-unmatched` | bool | also return objects that did not match the filters (`regex`, `template`) | false |
| `--paged` | bool | fetch objects page by page and print the next page immediately after it is received. Can be useful to quick check the bucket content. Ignored in fast mode | false |
| `--max-pages` | int | limit the number or displayed pages in paged mode, 0 - display all pages | 0 |
| `--marker` | string | start listing bucket objects from the object that follows the marker(in alphabetical order). Can be useful if used with limit flag to quick list a few objects starting from a given object. Ignored in fast mode | "" |

**Example:**

* `ais bucket objects mylocalbucket --prefix "mytestfolder/" --regex ".txt`
Returns all objects matching `.txt` under the `mytestfolder` directory from `mylocalbucket` bucket
* `AIS_BUCKET=mylocalbucket ais bucket objects --template="shard-{0..99}.tgz" --show-unmatched`
Returns all objects with names from `shard-0.tgz` to `shard-99.tgz` from `mylocalbucket`.
Also returns a separate list of objects that do not match the template.
* `ais bucket objects mylocalbucket --limit 10`
Displays first 10 objects of `mylocalbucket`
* `ais bucket objects mylocalbucket --limit 10 --marker "imageset/object0100"`
Displays the next 10 objects of bucket `mylocalbucket` that follow alphabetically object `"imageset/object0100"`: `imageset/object0101`, `imageset/object0102` etc
* `ais bucket objects mylocalbucket --paged --props=all -H`
Prints out the entire list of bucket objects with all properties page by page. Every page is displayed immediately after it is fetched. Header is off(`-H`) that makes the output easy to parse with scripts. Press Ctrl+C to interrupt.

### summary

`ais bucket summary BUCKET`

Displays aggregated information about objects in the `BUCKET`.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | string | pattern for names of objects used for aggregation | `""` |
| `--prefix` | string | prefix for named of objects used for aggregation | `""` |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |

### makencopies

`ais bucket makencopies BUCKET --copies <value>`

Starts an extended action (xaction) to bring a given bucket to a certain redundancy level (num copies). Read more about this feature [here](../../docs/storage_svcs.md#n-way-mirror).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--copies` | int | number of copies | `1` |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |

### props list

`ais bucket props ls BUCKET`

Lists [properties](../../docs/bucket.md#properties-and-options) of the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |

### props set

`ais bucket props set BUCKET <list of key=value | --jsonspec <json>>`

Sets bucket properties. For the available options, see [bucket-properties](../../docs/bucket.md#properties-and-options).
If `--jsonspec` is used **all** properties of the bucket are set based on the values in the JSON.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider)|
| `--jsonspec` | string | bucket properties in a JSON format | `` |

When `--jsonspec` is not used, some properties support user-friendly aliases:

| Property | Value alias | Description |
| --- | --- | --- |
| aattrs | ro | Disables bucket modifications: denies PUT, DELETE, and ColdGET requests |
| aattrs | rw | Enables bucket modifications: allows PUT, DELETE, and ColdGET requests |

**Examples:**

* `ais bucket props set mybucket 'mirror.enabled=true' 'mirror.copies=2'`

Sets the `mirror.enabled` and `mirror.copies` properties to `true` and `2` respectively.

* `ais bucket props set mybucket 'aattrs=ro'`

Sets read-only access to the bucket `mybucket`. All PUT and DELETE requests will fail.

* Setting **all** bucket attributes based on the provided JSON specification
```bash
ais bucket props set mybucket --jsonspec '{
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
After which `ais bucket props list mybucket` results in:
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

* If not all properties are mentioned in the JSON, the missing ones are set to zero values (empty/false/nil):
```bash
ais bucket props set mybucket --jsonspec '{
    "cloud_provider": "ais",
    "versioning": {
      "type": "own",
      "enabled": true,
      "validate_warm_get": true
    }
}'
```
After which `ais bucket props list mybucket` results in:
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

To see how setting zero values affect properties, you can run `ais bucket props set mybucket --jsonspec '{}'`

### props reset

`ais bucket props reset BUCKET`

Reset bucket properties to cluster default.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |

### Default `bucket` argument value
If you set `AIS_BUCKET` environment variable you can omit the argument that represents the name of the bucket (`BUCKET`) in
all of the commands above. For example, the following pairs of commands have the same effect:
 * `AIS_BUCKET=mybucket ais bucket create` and `ais bucket create mybucket`
 * `AIS_BUCKET=mybucket ais bucket rename mybucket1` and `ais bucket rename mybucket mybucket1`
 * `AIS_BUCKET=mybucket ais bucket props set 'aattrs=ro'` and `AIS_BUCKET=mybucket ais bucket props set mybck 'aattrs=ro'`
