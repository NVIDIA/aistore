## Object

The CLI allows users to interact with objects in the AIS cluster.

### GET

`ais get BUCKET_NAME/OBJECT_NAME OUT_FILE`

Gets the object from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |
| `--is-cached` | `bool` | Check if the object is cached locally, without downloading it. | `false` |

`OUT_FILE`: filename in already existing directory or `-` for `stdout`

#### Examples

| Command | Explanation |
| --- | --- |
| `ais get mybucket/myobj.txt '~/obj.txt'` | Gets object `myobj.txt` from bucket `mybucket` and writes it to the file `~/obj.txt` |
| `ais get mybucket/myobj.txt -` | Gets object `myobj.txt` from bucket `mybucket` and writes it to the standard output |
| `ais get --is-cached mybucket/myobj.txt` | Checks if object `myobj.txt` from bucket `mybucket` is cached locally |
| `ais get mybucket/myobj.txt '~/obj.txt' --offset 1024 --length 1024` | Gets contents of object `myobj.txt` starting from offset `1024` and having length `1024B` |

### SHOW

`ais show object BUCKET_NAME/OBJECT_NAME [PROP_LIST]`

Gets object detailed information. `PROP_LIST` is a comma separated list of properties to display.
If `PROP_LIST` is omitted default properties are shown(all except `provider` property).

Supported properties:

- `provider` - provider of the object's bucket, `ais` returned if local bucket
- `iscached` - is the object cached on local drives (always `true` for AIS buckets)
- `size` - object size
- `version` - object version (it is empty if versioning is disabled for the bucket)
- `atime` - object's last access time
- `copies` - the number of object replicas per target (empty if bucket mirroring is disabled)
- `checksum` - object's checksum
- `ec` - object's EC info (empty if EC is disabled for the bucket, if EC is enabled it looks like `DATA:PARITY[MODE]`, where `DATA` - the number of data slices, `PARITY` - the number of parity slices, and `MODE` is protection mode selected for the object: `replicated` - object has `PARITY` replicas on other targets, `encoded`  the object is erasure coded and other targets contains only encoded slices

#### Examples

`ais show object mybucket/myobj.txt`

Display all properties of object `myobj.txt` from bucket `mybucket`. Output example:

```shell
$ ais show object mybucket/myobj.txt
Checksum                Size    Atime                   Iscached        Version Copies  Ec
2d61e9b8b299c41f        7.63MiB 06 Jan 20 14:55 PST     true            1       1       2:2[encoded]
```

Show only selected properties:

```
$ ais show object mybucket/myobj2.txt -props size,version,ec`
Size    Version Ec
7.63MiB 1       2:2[replicated]
```


### PUT

`ais put FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup id="a1">[1](#ft1)</sup>

Puts a file or a directory content into the bucket. If CLI detects that a user is going to put more than one file, it calculates the total number or files, total data size and checks if bucket is empty, then shows all gathered info to the user and ask for confirmation to continue. Confirmation request can be disabled with option `--yes` for use in scripts.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--base` | `string` | Prefix that is removed when constructing object name from file name. Used if `OBJECT_NAME` is not given set <sup id="a2">[2](#ft2)</sup> | `""` |
| `--verbose` or `-v` | `bool` | Enable printing the result of every PUT | `false` |
| `--yes` or `-y` | `bool` | Answer `yes` to every confirmation prompt | `false` |
| `--conc` | `int` | Number of concurrent `PUT` requests limit | `10` |
| `--recursive` or `-r` | `bool` | Enable recursive directory upload | `false` |
| `--refresh` | `string` | Frequency of the reporting the progress (in milliseconds), may contain multiplicative suffix `s`(second) or `m`(minute). Zero value disables periodical refresh | `0` if verbose mode is on, `5s` otherwise |

<a name="ft1">1</a> `FILE|DIRECTORY` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards.
 Symbols `*` and `?` can be used only in a file name pattern. Directory names cannot include wildcards. Only a file name is matched, not full file path, so `/home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory.
 This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `/home/user/img-set-*/*.tar` or `/home/user/bck/**/*.tar.gz`

<a name="ft2">2</a> Option `--base` and argument `OBJECT_NAME` are mutually exclusive and `OBJECT_NAME` has higher priority. When `OBJECT_NAME` is given, options `--base` and `--recursive` are ignored, and `FILE` must point to an existing file. File masks and directory uploading are not supported in single-file upload mode.

### PROMOTE

`ais promote FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup id="a1">[1](#ft1)</sup>

Promote **AIS-colocated** files and directories to AIS objects in a specified bucket. Colocation in the context means that the files in question are already located *inside* AIStore (bare-metal or virtual) storage servers (targets).

For instance, let's say some (or all) of the deployed storage nodes contain a directory called `/tmp/examples`. To make AIS objects out of this directory's files (**one file = one object**), run:

```sh
$ ais promote /tmp/examples mybucket/ -r
```

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--base` | `string` | Pathname prefix that is omitted i.e., not used to generate object names | `""` |
| `--verbose` or `-v` | `bool` | Verbose printout | `false` |
| `--target` | `string` | Target ID; if specified, only the file/dir content stored on the corresponding AIS target is promoted | `""` |
| `--recursive` or `-r` | `bool` | Promote nested directories | `false` |
| `--overwrite` or `-o` | `bool` | Overwrite destination (object) if exists | `false` |

> The capability is intended to support existing toolchains that operate on files. Here's the rationale:

>> On the one hand, it is easy to transform files using `tar`, `gzip` and any number of other very familiar Unix tools. On the other hand, it is easy to **promote** files and directories that are locally present inside AIS servers. Once the original file-based content becomes distributed across AIStore cluster, running massive computations (or any other workloads that require scalable storage) also becomes easy and fast.

> NOTE: advanced usage only!

#### Object names

While uploading `ais` assigns names to object by following the rules (staring with the highest priority):

1. If `OBJECT_NAME` is set, the object's name is the value of `OBJECT_NAME`
2. If `--base` is set, the object's name is the file path without leading `--base` prefix. A trailing `/` in `--base` can be omitted.
3. If neither `OBJECT_NAME` nor `--base` is defined, the name of the object is the filename - the last element of the path.

Be careful when putting a directory recursively without setting `--base`: it may result in overwriting objects with the same names.

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

| Command | Explanation |
| --- | --- |
| `ais put "/home/user/bck/img1.tar" mybucket/img-set-1.tar` | Puts a single file `img1.tar` into bucket `mybucket` as `img-set-1.tar` |
| `ais put "/home/user/bck" mybucket/` | Puts two objects, `img1.tar` and `img2.zip`, into the root of bucket `mybucket`. Note that the path `/home/user/bck` is a shortcut for `/home/user/bck/*` and that recursion is disabled by default |
| `ais put "/home/user/bck" mybucket/ --base ~/bck --recursive` | `--base` is expanded with user's home directory into `/home/user/bck`, so the final bucket content is `img1.tar`, `img2.zip`, `extra/img1.tar` and `extra/img3.zip` |
| `ais put "~/bck/*.tar" mybucket/ --base ~/bck --recursive` | Same as above, except only files matching pattern `*.tar` are put, so the final bucket content is `img1.tar` and `extra/img1.tar` |

##### Bad examples

| Command | Explanation |
| --- | --- |
| `ais put "/home/user/bck/*.tar" mybucket/img1.tar --base ~/bck --recursive` | `OBJECT_NAME` has priority over `--base` and `--recursive` and `/home/user/bck/*.tar` is not a valid file, so the `PUT` fails |

### Delete

`ais rm object BUCKET_NAME/[OBJECT_NAME]...`

Deletes an object or list/range of objects from the bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion <sup id="a3">[3](#ft3) | `""` |
| `--range` | `string` | Start and end interval (eg. 1:100) for range deletion <sup id="a4">[3](#ft3) | `""` |
| `--prefix` | `string` | Prefix for range deletion | `""` |
| `--regex` | `string` | Regex for range deletion | `""` |
| `--deadline` | `string` | Time duration before the request expires [(Go's time.Duration string)](https://golang.org/pkg/time/#Duration.String) | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | `bool` | Wait for operation to finish before returning response | `true` |

<a name="ft3">3</a> Options `--list,--range` and argument(s) `OBJECT_NAME` are mutually exclusive. List and range deletions expect only a bucket name; if one or more
`OBJECT_NAME`s are given, a separate `DELETE` request is sent for each object.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais rm object mybucket/myobj` | Deletes object `myobj` from bucket `mybucket` |
| `ais rm object aisbck/obj1 cloudbck/obj2` | Deletes two objects (`obj1`, `obj2`) from different buckets (`aisbck`, `cloudbck`) |
| `ais rm object mybucket/ --list "obj1,obj2,obj3"` | Deletes a list of objects (`obj1`, `obj2`, `obj3`) from bucket `mybucket` |
| `ais rm object mybucket/ --range "1:3" --prefix "test-" --regex "\\d\\d\\d"` | Deletes objects in range `001-003`, with prefix `test-`, matching `[0-9][0-9][0-9]` regex, from bucket `mybucket` |

### Evict

`ais evict BUCKET_NAME/[OBJECT_NAME]...`

[Evicts](../../docs/bucket.md#prefetchevict-objects) objects from a cloud bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion <sup id="a5">[4](#ft4) | `""` |
| `--range` | `string` | Start and end interval (eg. 1:100) for range deletion <sup id="a6">[4](#ft4) | `""` |
| `--prefix` | `string` | Prefix for range deletion | `""` |
| `--regex` | `string` | Regex for range deletion | `""` |
| `--deadline` | `string` | Time duration before the request expires [(Go's time.Duration string)](https://golang.org/pkg/time/#Duration.String) | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | `bool` | Wait for operation to finish before returning response | `true` |

<a name="ft4">4</a> Options `--list,--range` and argument(s) `OBJECT_NAME` are mutually exclusive. List and range evictions expect only a bucket name; if one or more
`OBJECT_NAME`s are given, a separate eviction request is sent for each object.

### Prefetch

`ais prefetch BUCKET_NAME/ --list|--range <value>`

[Prefetches](../../docs/bucket.md#prefetchevict-objects) objects from the cloud bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion <sup id="a5">[4](#ft4) | `""` |
| `--range` | `string` | Start and end interval (eg. 1:100) for range deletion <sup id="a6">[4](#ft4) | `""` |
| `--prefix` | `string` | Prefix for range deletion | `""` |
| `--regex` | `string` | Regex for range deletion | `""` |
| `--deadline` | `string` | Time duration before the request expires [(Go's time.Duration string)](https://golang.org/pkg/time/#Duration.String) | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | `bool` | Wait for operation to finish before returning response | `true` |

### Rename

`ais rename object BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME`

Renames object from an ais bucket.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais rename object mybucket/obj obj1` | Renames object `obj` from bucket `mybucket` to `obj1` |

### Compose

`ais compose DIRNAME|FILENAME [DIRNAME|FILENAME...] BUCKET/OBJECT_NAME`

Creates an object in a bucket by concatenating provided files, keeping the order as in the arguments list.
If directory provided, files within directory are sorted by filename.
For each file sends separate request to the cluster.
Supports recursive iteration through directories and wildcards in the same way as PUT operation does. 

| Command | Explanation |
| --- | --- |
| `ais compose file1.txt dir/file2.txt mybucket/obj` | In two separate requests sends `file1.txt` and `dir/file2.txt` to the cluster, concatenates the files keeping the order and saves them as `obj` in bucket `mybucket`  |
| `ais compose file1.txt dir/file2.txt mybucket/obj --verbose` | Same as above, but additionally shows progress bar of sending the files to the cluster  |

| `ais compose dirB dirA mybucket/obj` | Creates `obj` in bucket `mybucket` which is concatenation of sorted files from `dirB` with sorted files from `dirA` |