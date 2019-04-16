## Bucket

The CLI allows for users to interact with [buckets](../../docs/bucket.md) in the AIS cluster.

## Command List

### create

`ais bucket create --bucket <value>`

Creates a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to be created | `""` |


### delete

`ais bucket destroy --bucket <value>`

Destroys a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to be deleted | `""` |


### evict

`ais bucket evict --bucket <value>`

Evicts a cloud bucket. It also resets the properties of the bucket (if changed).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the cloud bucket to be evicted | `""` |


### getprops

`ais bucket getprops --bucket <value>`

Gets the [properties](../../docs/bucket.md#properties-and-options) of the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](../README.md#enums) | locality of bucket | `""` |


### list

`ais bucket list --bucket <value>`

Lists all the objects along with some of the objects' properties. For the full list of properties, see [here](../../docs/bucket.md#list-bucket).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--props` | string | comma separated list of properties to return with object names | `size,version` |
| `--regex` | string | pattern for object matching | `""` |
| `--prefix` | string | prefix for object matching | `""` |
| `--pagesize` | string | maximum number of object names returned in response | `1000` (cloud), `65536` (local) |
| `--limit` | string | limit of object count | `0` (unlimited) |
| `--bprovider` | [Provider](../README.md#enums) | locality of bucket | `""` |

**Example:**

`ais bucket list --bucket mylocalbucket --prefix "mytestfolder/" --regex ".txt`

Returns all objects matching `.txt` under the `mytestfolder` directory from `mylocalbucket` bucket


### makencopies

`ais bucket makencopies --bucket <value> --copies <value>`

Starts an extended action (xaction) to bring a given bucket to a certain redundancy level (num copies). Read more about this feature [here](../../docs/storage_svcs.md#n-way-mirror).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--copies` | int | number of copies | `1` |
| `--bprovider` | [Provider](../README.md#enums) | locality of bucket | `""` |

### names

`ais bucket names`

Returns the names of the buckets.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | string | pattern for bucket matching | `""` |
| `--bprovider` | [Provider](../README.md#enums) | returns `local` or `cloud` buckets. If empty, returns all bucket names. | `""` |

### rename

`ais bucket rename --bucket <value> --newbucket <value> `

Renames a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | old name of the bucket | `""` |
| `--newbucket` | string | new name of the bucket | `""` |

### resetprops

`ais bucket resetprops --bucket <value>`

Reset bucket properties to cluster default.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](../README.md#enums) | locality of bucket | `""` |

### setprops

`ais bucket setprops --bucket <value> [list of key=value]`

Sets bucket properties. For the available options, see [bucket-properties](../../docs/bucket.md#properties-and-options).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket | `""` |
| `--bprovider` | [Provider](../README.md#enums) | locality of bucket | `""` |
| `--json` | bool | use json as input (need set all bucket props) | `false` |

**Example:**

`ais bucket setprops --bucket mybucket 'mirror.enabled=true' 'mirror.copies=2'`

Sets the `mirror.enabled` and `mirror.copies` properties to `true` and `2` respectively.
