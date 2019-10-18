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

#### Examples

| Command | Explanation |
| --- | --- |
| `ais get mybucket/myobj.txt '~/obj.txt'` | Gets object `myobj.txt` from bucket `mybucket` and writes it to the file `~/obj.txt` |
| `ais get mybucket/myobj.txt -` | Gets object `myobj.txt` from bucket `mybucket` and writes it to the standard output |
| `ais get --is-cached mybucket/myobj.txt` | Checks if object `myobj.txt` from bucket `mybucket` is cached locally |
| `ais get mybucket/myobj.txt '~/obj.txt' --offset 1024 --length 1024` | Gets contents of object `myobj.txt` starting from offset `1024` and having length `1024B` |

### PUT

`ais put FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup id="a1">[1](#ft1)</sup>

Puts a file or a directory content into the bucket. If `ais` detects that a user is going to put more than one file, it calculates the total number or files, total data size and checks if bucket is empty,  then shows all gathered info to the user and ask for confirmation to continue. Confirmation request can be disabled with option `--yes` for use in scripts.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--base` | `string` | Prefix that is removed when constructing object name from file name. Used if `OBJECT_NAME` is not given set <sup id="a2">[2](#ft2)</sup> | `""` |
| `--verbose` or `-v` | `bool` | Enable printing the result of every PUT | `false` |
| `--yes` or `-y` | `bool` | Answer `yes` to every confirmation prompt | `false` |
| `--conc` | `int` | Number of concurrent `PUT` requests limit | `10` |
| `--recursive` or `-r` | `bool` | Enable recursive directory upload | `false` |
| `--refresh` | `string` | Frequency of the reporting the progress (in milliseconds), may contain multiplicative suffix `s`(second) or `m`(minute). Zero value disables periodical refresh | `0` if verbose mode is on, `5s` otherwise |

<a name="ft1">1</a> `FILE|DIRECTORY` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards: symbols `*` and `?` can be used only in file name pattern, directory names cannot include wildcards. Also `ais` always matches a file name only, not full file path, so `/home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory. This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `/home/user/img-set-*/*.tar` or `/home/user/bck/**/*.tar.gz`

<a name="ft2">2</a> Option `--base` and argument `OBJECT_NAME` are mutually exclusive and `OBJECT_NAME` has higher priority. When `OBJECT_NAME` is given, options `--base` and `--recursive` are ignored, and `FILE` must point to an existing file. File masks and directory uploading are not supported in single-file upload mode.

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
