## Object 

The CLI allows for users to interact with objects in the AIS cluster.

## Command List

### get

`ais object get --bucket <value> --key <value>`

Gets the object from the bucket. If `--outfile` is empty, it stores the file in a locally cached version in the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to retrieve the object | `""` |
| `--key` | string | key of the object | `""` |
| `--outfile` | string | name of the file to store the contents of the object | `""` |
| `--offset` | string | read offset | `""` |
| `--length` | string | read length |  `""` |
| `--bprovider` | [Provider](../README.md#enums) | locality of the bucket | `""` |
| `--checksum` | bool | validate the checksum of the object | `false` |
| `--props` | bool | returns the properties of object (size and version). It does not download the object. | `false` |

**Examples:**

`ais object get --bucket mycloudbucket --key mycloudobj.txt --outfile "~/obj.txt"`

Gets `mycloudobj.txt` from `mycloudbucket` and stores it in `obj.txt` in the `HOME` directory.

`ais object get --bucket mylocalbucket --key obj.txt --props`

Returns the properties of `obj.txt` without downloading the object.

### put

`ais object put --bucket <value> --key <value> --body <value>`

Put an object into the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to put the object | `""` |
| `--key` | string | key of the object | `""` |
| `--body` | string | file that contains the contents of the object | `""` |
| `--bprovider` | [Provider](../README.md#enums) | locality of the bucket | `""` |

**Example:**

`ais object put --bucket mybucket --key "newfile.txt" --body "existingfile.txt"`

Put `existingfile.txt` into `mybucket` as `newfile.txt`.

### delete

`ais object delete --bucket <value> --key <value>`

Deletes an object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that contains the object | `""` |
| `--key` | string | key of the object | `""` |
| `--list` | string | comma separated list of objects for list delete| `""` |
| `--range` | string | start and end interval (eg. 1:100) for range delete | `""` |
| `--prefix` | string | prefix for range delete | `""` |
| `--regex` | string | regex for range delete | `""` |
| `--deadline` | string | amount of time (Go Duration string) before the request expires | `0s` (no deadline) |
| `--bprovider` | [Provider](../README.md#enums) | locality of the bucket | `""` |
| `--wait` | bool | wait for operation to finish before returning response | `true` |

**Examples:**

`ais object delete --bucket mybucket --list "obj1,obj2,obj3"`

Deletes the three objects `obj1`, `obj2`, and `obj3`.

`ais object delete --bucket rlin-test-1 --range "1:3" --prefix "test-" --regex "\\d\\d\\d"`

Deletes the objects in the range `001` to `003` with prefix `test-` matching the `[0-9][0-9][0-9]` expression.

### rename

`ais object rename --bucket <value> --key  <value> --newkey <value>`

Rename object from a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that holds the object | `""` |
| `--key` | string | old name of object | `""` |
| `--newkey` | string | new name of object | `""` |

**Example:**

`ais object rename --bucket mylocalbucket --key "oldfile.txt" --newkey "newfile.txt"`

Renames `oldfile.txt` from `mylocalbucket` to `newfile.txt`.