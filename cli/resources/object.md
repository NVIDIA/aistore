## Object 

The CLI allows users to interact with objects in the AIS cluster.

## Command List

### get

`ais object get --bucket <value> --name <value> --out-file <value>`

Gets the object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to retrieve the object | `""` or [default](../README.md#bucket) |
| `--name` | string | name of the object | `""` |
| `--out-file` | string | name of the file to store the contents of the object, can be set to `-` to print to STDOUT | `""` |
| `--offset` | string | read offset, can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | string | read length, can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--checksum` | bool | validate the checksum of the object | `false` |
| `--cached` | bool | checks if the object is cached locally. It does not download the object. | `false` |

**Examples:**

`ais object get --bucket mycloudbucket --name mycloudobj.txt --out-file "~/obj.txt"`

Gets `mycloudobj.txt` from `mycloudbucket` and saves it in `obj.txt` in the `HOME` directory.

`AIS_BUCKET=mylocalbucket ais object get --name obj.txt --out-file -`

Gets `obj.txt` from `mycloudbucket` and prints its content to STDOUT.

### put

`ais object put --bucket <value> --name <value> --file <value>`

Put an object into the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to put the object | `""` or [default](../README.md#bucket) |
| `--name` | string | name of the object | `last element on the path of --file argument` |
| `--file` | string | file that contains the contents of the object | `""` |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |

**Example:**

`ais object put --bucket mybucket --name "newfile.txt" --file "existingfile.txt"`

Put `existingfile.txt` into `mybucket` as `newfile.txt`.

### delete

`ais object delete --bucket <value> --name <value>`

Deletes an object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that contains the object | `""` or [default](../README.md#bucket) |
| `--name` | string | name of the object | `""` |
| `--list` | string | comma separated list of objects for list delete| `""` |
| `--range` | string | start and end interval (eg. 1:100) for range delete | `""` |
| `--prefix` | string | prefix for range delete | `""` |
| `--regex` | string | regex for range delete | `""` |
| `--deadline` | string | amount of time [(Go Duration string)](https://golang.org/pkg/time/#Duration.String) before the request expires | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | bool | wait for operation to finish before returning response | `true` |

**Examples:**

`ais object delete --bucket mybucket --list "obj1,obj2,obj3"`

Deletes the three objects `obj1`, `obj2`, and `obj3`.

`ais object delete --bucket rlin-test-1 --range "1:3" --prefix "test-" --regex "\\d\\d\\d"`

Deletes the objects in the range `001` to `003` with prefix `test-` matching the `[0-9][0-9][0-9]` expression.

### evict

`ais object evict --bucket <value> --name <value>`

[Evicts](../../docs/bucket.md#prefetchevict-objects) objects from cloud bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that contains the object | `""` or [default](../README.md#bucket) |
| `--name` | string | name of the object | `""` |
| `--list` | string | comma separated list of objects for list eviction| `""` |
| `--range` | string | start and end interval (eg. `1:100`) for range eviction | `""` |
| `--prefix` | string | prefix for range eviction | `""` |
| `--regex` | string | regex for range eviction | `""` |
| `--deadline` | string | amount of time [(Go Duration string)](https://golang.org/pkg/time/#Duration.String) before the request expires | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | bool | wait for operation to finish before returning response | `true` |

**Examples:**

`ais object evict --bucket mybucket --range "1:10" --prefix "testfldr/test-" --regex "\\d\\d\\d"`

Evicts the range of objects from `001` to `010` with the matching prefix `testfld/test-` that has the format `[0-9][0-9][0-9]` in the name.


### prefetch

`ais object prefetch --bucket <value> --list <value>`

[Prefetches](../../docs/bucket.md#prefetchevict-objects) objects from the cloud bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that contains the object | `""` or [default](../README.md#bucket) |
| `--list` | string | comma separated list of objects for list prefetch| `""` |
| `--range` | string | start and end interval (eg. `1:100`) for range prefetch | `""` |
| `--prefix` | string | prefix for range prefetch | `""` |
| `--regex` | string | regex for range prefetch | `""` |
| `--deadline` | string | amount of time [(Go Duration string)](https://golang.org/pkg/time/#Duration.String) before the request expires | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | bool | wait for operation to finish before returning response | `true` |

**Examples:**

`ais object prefetch --bucket mybucket --list "test1.txt, test2.txt, test3.txt"`

Prefetches the list of objects (`test1.txt`, `test2.txt`, `test3.txt`) from the bucket.

### rename

`ais object rename --bucket <value> --name  <value> --new-name <value>`

Rename object from a local bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket that holds the object | `""` or [default](../README.md#bucket) |
| `--name` | string | old name of object | `""` |
| `--new-name` | string | new name of object | `""` |

**Example:**

`ais object rename --bucket mylocalbucket --name "oldfile.txt" --new-name "newfile.txt"`

Renames `oldfile.txt` in `mylocalbucket` to `newfile.txt`.
