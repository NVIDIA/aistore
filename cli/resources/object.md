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

`ais object put --bucket <value> --name <value> --file <value> --verbose --yes --base <value> --conc <value> --refresh <value>`

Put an object or a directory content into the bucket. If `ais` detects that a user is going to put more than one file, it calculates the total number or files, total data size, checks if bucket is empty, after that `ais` shows all gathered info to the user and ask for confirmation to continue. Confirmation request can be disabled with option `--yes` to use the command in scripts.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--bucket` | string | name of the bucket to put the object | `""` or [default](../README.md#bucket) |
| `--name` | string | name of the object | `last element on the path of --file argument` |
| `--file` | string | file, file mask or directory that contains the content of the object(s) <sup id="a1">[1](#ft1)</sup> | `""` |
| `--provider` | [Provider](../README.md#enums) | locality of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--base` | string | prefix that is removed when constructing object name from file name. Used if `--name` is not set <sup id="a2">[2](#ft1)</sup> | `""` |
| `--verbose` or `-v` | bool | enables printing the result of every PUT | false |
| `--yes` or `-y` | bool | automatic "yes" response to every confirmation | false |
| `--conc` | `int` | limits number of concurrent put requests | `10` |
| `--recursive` or `-r` | bool | uploads the directory recursively | false |
| `--refresh` | string | frequency of the reporting the progress (in milliseconds), may contain multiplicative suffix `s`(second) or `m`(minute). Zero value disables periodical refresh | `0` if verbose mode is on, `5s` otherwise |

<a name="ft1">1</a> `--file` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards: symbols `*` and `?` can be used only in file name pattern, directory names cannot include wildcards. Also `ais` always matches a file name only, not full file path, so `--file /home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory. This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `--file /home/user/img-set-*/*.tar` or `--file /home/user/bck/**/*.tar.gz`

<a name="ft2">2</a> Options `--name` and `--base` are mutually exclusive and `--name` has higher priority. When `--name` is defined, options `--base` and `--recursive` are ignored, and `--file` must point to existing file. File masks and directory uploading are not supported in single-file upload mode.

#### Object names

While uploading `ais` assigns names to object by following the rules (staring with the highest priority):

1. If `--name` is set, the object's name is the value of the `--name`
2. If `--base` is set, the object's name is the file path without leading `--base` prefix. A trailing `/` in `--base` can be omitted.
3. If neither `--name` nor `--base` is defined, the name of the object is the filename - the last element of the path.

Be careful when putting a directory recursively without setting `--base`: it may result in overwriting objects with same names.

#### Examples

All examples below put into an empty bucket and the source directory structure is:

```
/home/user/bck/img1.tar
/home/user/bck/img2.zip
/home/user/bck/extra/img1.tar
/home/user/bck/extra/img3.zip
```

The current user HOME directory is `/home/user`.

##### Good examples

1. `ais object put --bucket mybucket --name "img-set-1.tar" --file "/home/user/bck/img1.tar"`

Put a single file `img1.tar` into `mybucket` as `img-set-1.tar`.

2. `ais object put --file /home/user/bck --bucket mybucket`

Puts two objects `img1.tar` and `img2.zip` into the root of `mybucket`. The path `--file /home/user/bck` is a shortcut for `--file /home/user/bck/*` - `ais` detects that the directory name is passed and starts directory content uploading. Recursion is disabled by default.

3. `ais object put --file /home/user/bck --base ~/bck --bucket mybucket --recursive`

It expands `--base` with user's home directory into `/home/user/bck`. The final bucket content is: `img1.tar`, `img2.zip`, `extra/img1.tar`, and `extra/img3.zip`.

4. `ais object put --file ~/bck/*.tar --base ~/bck --bucket mybucket --recursive`

It works similar to previous example, but only files matching pattern `*.tar` are put. It results in a bucket with 2 objects: `img1.tar` and `extra/img1.tar`.

##### Bad examples

1. `ais object put --file /home/user/bck/*.tar --base ~/bck --bucket mybucket --recursive --name img1.tar`

The custom object name `--name img1.tar` is detected, so `ais` starts uploading a single file. Due to `/home/user/bck/*.tar` is invalid file name the PUT operation fails.

2. `ais object put --file /home/user/bck --bucket mybucket --recursive`

Since `--base` is not set, the name of object is the filename. It results that two object names conflicts, and the final bucket content is only 3 objects: `img1.tar`, `img2.zip`, and `img3.zip` because `img1.tar` from the root was overwritten with the file `extra/img1.tar`.


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
