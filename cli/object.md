---
layout: page
title: Object commands
permalink: /cli/object/
---

## Operations on objects

### GET

`ais get BUCKET_NAME/OBJECT_NAME OUT_FILE`

GET object from bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |
| `--is-cached` | `bool` | Check if the object is cached locally, without downloading it. | `false` |

`OUT_FILE`: filename in already existing directory or `-` for `stdout`

#### Examples

1) GET object `myobj.txt` from bucket `mybucket` and write it as file `~/obj.txt`

```sh
$ ais get mybucket/myobj.txt ~/obj.txt
```

2) GET object `myobj.txt` from bucket `mybucket` and write it to standard output

```sh
$ ais get mybucket/myobj.txt -
```

3) Check if object `myobj.txt` from bucket `mybucket` is cached locally

```sh
$ ais get --is-cached mybucket/myobj.txt
```

4) Read Range: GET contents of object `myobj.txt` starting from offset `1024` length `1024`

```sh
$ ais get mybucket/myobj.txt ~/obj.txt --offset 1024 --length 1024
```

### SHOW

`ais show object BUCKET_NAME/OBJECT_NAME [PROP_LIST]`

Get object detailed information.
`PROP_LIST` is a comma-separated list of properties to display.
If `PROP_LIST` is omitted default properties are shown (all except `provider` property).

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

```sh
$ ais show object mybucket/myobj.txt
Checksum                Size    Atime                   Iscached        Version Copies  Ec
2d61e9b8b299c41f        7.63MiB 06 Jan 20 14:55 PST     true            1       1       2:2[encoded]
```

Show only selected properties:

```sh
$ ais show object mybucket/myobj2.txt -props size,version,ec`
Size    Version Ec
7.63MiB 1       2:2[replicated]
```

### PUT

`ais put FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup id="a1">[1](#ft1)</sup>

PUT an object or entire directory (of objects) into the specified bucket. If CLI detects that a user is going to put more than one file, it calculates the total number of files, total data size and checks if the bucket is empty, then shows all gathered info to the user and asks for confirmation to continue. Confirmation request can be disabled with the option `--yes` for use in scripts.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--trim-prefix` | `string` | Prefix that is removed when constructing object name from file name. Used if `OBJECT_NAME` is not given set <sup id="a2">[2](#ft2)</sup> | `""` |
| `--verbose` or `-v` | `bool` | Enable printing the result of every PUT | `false` |
| `--yes` or `-y` | `bool` | Answer `yes` to every confirmation prompt | `false` |
| `--conc` | `int` | Number of concurrent `PUT` requests limit | `10` |
| `--recursive` or `-r` | `bool` | Enable recursive directory upload | `false` |
| `--refresh` | `string` | Frequency of the reporting the progress (in milliseconds), may contain multiplicative suffix `s`(second) or `m`(minute). Zero value disables periodical refresh | `0` if verbose mode is on, `5s` otherwise |
| `--dry-run` | `bool` | Do not actually perform PUT. Shows a few files to be uploaded and corresponding object names for used arguments

<a name="ft1">1</a> `FILE|DIRECTORY` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards.
 Symbols `*` and `?` can be used only in a file name pattern. Directory names cannot include wildcards. Only a file name is matched, not full file path, so `/home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory.
 This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `/home/user/img-set-*/*.tar` or `/home/user/bck/**/*.tar.gz`

<a name="ft2">2</a> Option `--trim-prefix` and argument `OBJECT_NAME` are mutually exclusive and `OBJECT_NAME` has higher priority. When `OBJECT_NAME` is given, options `--trim-prefix` and `--recursive` are ignored, and `FILE` must point to an existing file. File masks and directory uploading are not supported in single-file upload mode.

#### Object names

PUT command handles two possible ways to specify resulting object names:
- Object name is not provided: `ais put path/to/(..)/file.go bucket/` creates object `file.go` in `bucket`
- Explicit object name is provided: `ais put path/to/(..)/file.go bucket/path/to/object.go` creates object `path/to/object.go` in `bucket`

PUT command uses implicit object names if its source references multiple files:

- If source is specified as an absolute path, or is abbreviation like `~`, (for example `/tmp/*.go` or `~/dir`))
resulting objects names will be the same as the absolute path (`~` will be resolved to for example `/home/user`).
- If source is specified as relative path (for example `dir/*.go` or `dir/file{0..10}.txt`) resulting objects names
will be the same as the relative path.
- Abbreviations like `../` are not supported at the moment.

Leading prefix can be removed with `--trim-prefix`.

#### Examples

All examples below put into an empty bucket and the source directory structure is:

```
/home/user/bck/img1.tar
/home/user/bck/img2.zip
/home/user/bck/extra/img1.tar
/home/user/bck/extra/img3.zip
```

The current user HOME directory is `/home/user`.

1) PUT a single file `img1.tar` into bucket `mybucket`, name it `img-set-1.tar`
```sh
$ ais put "/home/user/bck/img1.tar" mybucket/img-set-1.tar

# PUT /home/user/bck/img1.tar => mybucket/img-set-1.tar
```

1) PUT a single file `~/bck/img1.tar` into bucket `mybucket`, without explicit name
```sh
$ ais put "~/bck/img1.tar" mybucket/

# PUT /home/user/bck/img1.tar => mybucket/home/user/bck/img-set-1.tar
```

2) PUT two objects, `/home/user/bck/img1.tar` and `/home/user/bck/img2.zip`, into the root of bucket `mybucket`. Note that the path `/home/user/bck` is a shortcut for `/home/user/bck/*` and that recursion is disabled by default
```sh
$ ais put "/home/user/bck" mybucket

# PUT /home/user/bck/img1.tar => mybucket/home/user/bck/img1.tar
# PUT /home/user/bck/img1.tar => mybucket/home/user/bck/img2.zip
```

3) `--trim-prefix` is expanded with user's home directory into `/home/user/bck`, so the final bucket content is `img1.tar`, `img2.zip`, `extra/img1.tar` and `extra/img3.zip`
```sh
$ ais put "/home/user/bck" mybucket/ --trim-prefix=~/bck --recursive

# PUT /home/user/bck/img1.tar => mybucket/img1.tar
# PUT /home/user/bck/img1.tar => mybucket/img2.zip
# PUT /home/user/bck/extra/img1.tar => mybucket/extra/img1.tar
# PUT /home/user/bck/extra/img3.zip => mybucket/extra/img3.zip
```

4) Same as above, except that only files matching pattern `*.tar` are PUT, so the final bucket content is `img1.tar` and `extra/img1.tar
```sh
$ ais put "~/bck/*.tar" mybucket/ --trim-prefix=~/bck --recursive

# PUT /home/user/bck/img1.tar => mybucket/img1.tar
# PUT /home/user/bck/extra/img1.tar => mybucket/extra/img1.tar
```

5) PUT 9 files to `mybucket` using range request. Object names formatted as `/home/user/dir/test${d1}${d2}.txt`
```shell script
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done
$ ais put "~/dir/test{0..2}{0..2}.txt" mybucket -y
9 objects put into "mybucket" bucket

# PUT /home/user/dir/test00.txt => /home/user/dir/test00.txt and 8 more
```

6) Same as above, except object names are in format `test${d1}${d2}.txt`
```shell script
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done
$ ais put "~/dir/test{0..2}{0..2}.txt" mybucket -y --trim-prefix=~/dir
9 objects put into "mybucket" bucket

# PUT /home/user/dir/test00.txt => test00.txt and 8 more
```


7) Preview the files that would be sent to the cluster, without really putting them.
```shell script
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done
$ ais put "~/dir/test{0..2}{0..2}.txt" mybucket --trim-prefix=~/dir --dry-run
[DRY RUN] No modifications on the cluster
/home/user/dir/test00.txt => mybucket/test00.txt
(...)
```

8) Put multiple directories into the cluster with range syntax
```shell script
$ for d1 in {0..10}; do mkdir dir$d1 && for d2 in {0..2}; do echo "0" > dir$d1/test${d2}.txt; done; done
$ ais put "dir{0..10}" mybucket -y
33 objects put into "mybucket" bucket

# PUT "/home/user/dir0/test0.txt" => b/dir0/test0.txt and 32 more
```

#### Example: invalid file

```sh
$ ais put "/home/user/bck/*.tar" mybucket/img1.tar --trim-prefix=~/bck --recursive
```

Note that `OBJECT_NAME` has a priority over flags (`--trim-prefix` and `--recursive`) and `/home/user/bck/*.tar` is not a valid because only single file is expected.

### PROMOTE

`ais promote FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup id="a1">[1](#ft1)</sup>

Promote **AIS-colocated** files and directories to AIS objects in a specified bucket.
Colocation in the context means that the files in question are already located *inside* AIStore (bare-metal or virtual) storage servers (targets).

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--trim-prefix` | `string` | Pathname prefix that is omitted i.e., not used to generate object names | `""` |
| `--verbose` or `-v` | `bool` | Verbose printout | `false` |
| `--target` | `string` | Target ID; if specified, only the file/dir content stored on the corresponding AIS target is promoted | `""` |
| `--recursive` or `-r` | `bool` | Promote nested directories | `false` |
| `--overwrite` or `-o` | `bool` | Overwrite destination (object) if exists | `false` |


#### Examples

1) Make AIS objects out of `/tmp/examples` files (**one file = one object**). `/tmp/examples` is a directory present on some (or all) of the deployed storage nodes. Created objects names have prefix `example`
```shell script
$ ais promote /tmp/examples mybucket/ -r
```

2) Promote /tmp/examples files to AIS objects. Objects names won't have `/tmp/examples` prefix
```shell script
$ ais promote /tmp/examples mybucket/ -r --trim-prefix=/tmp
```

3) Promote /tmp/examples/example1.txt as object with name `example1.txt`
```shell script
$ ais promote /tmp/examples/example1.txt mybucket/example1.txt

# PROMOTE /tmp/examples/example1.txt => mybucket/example1.txt
```

4) Promote /tmp/examples/example1.txt without specified object name
```shell script
$ ais promote /tmp/examples/example1.txt mybucket

# PROMOTE /tmp/examples/example1.txt => mybucket/tmp/examples/example1.txt
```

#### Example: no such file or directory

```shell script
$ ais create bucket testbucket
testbucket bucket created

$ ais status
Target		 %MemUsed	 MemAvail	 %CapUsed	 CapAvail	 %CpuUsed	 Rebalance
1014646t8081	   0.00		 4.00GiB	 50		 100.000GiB	   0.00		 finished; 0 objs moved (0B)
...
$ ais promote /target/1014646t8081/nonexistent/dir/ testbucket --target 1014646t8081 # directory doesn't exist on target 1014646t8081
(...) Bad Request: stat /target/1014646t8081/nonexistent/dir: no such file or directory
```

> The capability is intended to support existing toolchains that operate on files. Here's the rationale:

>> On the one hand, it is easy to transform files using `tar`, `gzip` and any number of other very familiar Unix tools. On the other hand, it is easy to **promote** files and directories that are locally present inside AIS servers. Once the original file-based content becomes distributed across AIStore cluster, running massive computations (or any other workloads that require scalable storage) also becomes easy and fast.

#### Object names

1. `PROMOTE` with directory as source
- `OBJECT_NAME` parameter is not supported
- If `--trim-prefix` is set, the object's name is the file path without leading `--trim-prefix`. A trailing `/` in `--trim-prefix` can be omitted.
- If `--trim-prefix` is not defined, resulting object names will be the same as full paths to files

Be careful when putting a directory recursively without setting `--trim-prefix`: it may result in overwriting objects with the same names.

2. `PROMOTE` with single file as source
- `--trim-prefix` flag is omitted
- If `OBJECT_NAME` provided it will be used as object name
- If `OBJECT_NAME` not provided, object name will be the same as the full path to the file, without leading `/`

### Delete

`ais rm object BUCKET_NAME/[OBJECT_NAME]...`

Delete an object or list/range of objects from the bucket.

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

1) Delete object `myobj` from bucket `mybucket`
```sh
$ ais rm object mybucket/myobj
```

2) Delete objects (`obj1`, `obj2`) from buckets (`aisbck`, `cloudbck`) respectively
```sh
$ ais rm object aisbck/obj1 cloudbck/obj2
```

3) Delete a list of objects (`obj1`, `obj2`, `obj3`) from bucket `mybucket`
```sh
$ ais rm object mybucket/ --list "obj1,obj2,obj3"
```

4) Delete all objects in range `001-003`, with prefix `test-`, matching `[0-9][0-9][0-9]` regex, from bucket `mybucket`
```sh
$ ais rm object mybucket/ --range "1:3" --prefix "test-" --regex "\\d\\d\\d"
```

### Evict

`ais evict BUCKET_NAME/[OBJECT_NAME]...`

[Evict](../../docs/bucket.md#prefetchevict-objects) objects from a cloud bucket.

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


#### Examples

```sh
$ ais put file.txt cloudbucket/file.txt
PUT file.txt into bucket cloudbucket
$ ais show bucket cloudbucket --cached # show only cloudbucket objects present in the AIS cluster
Name	    Objects	Size	Used(%)	Provider
cloudbucket	1	    702B	0%	    aws

$ ais evict cloudbucket/file.txt
file.txt evicted from cloudbucket bucket
$ ais show bucket cloudbucket --cached
Name	    Objects	Size	Used(%)	Provider
cloudbucket	0	    0B	    0%	    aws
```

### Prefetch

`ais prefetch BUCKET_NAME/ --list|--range <value>`

[Prefetch](../../docs/bucket.md#prefetchevict-objects) objects from the cloud bucket.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion <sup id="a5">[4](#ft4) | `""` |
| `--range` | `string` | Start and end interval (eg. 1:100) for range deletion <sup id="a6">[4](#ft4) | `""` |
| `--prefix` | `string` | Prefix for range deletion | `""` |
| `--regex` | `string` | Regex for range deletion | `""` |
| `--deadline` | `string` | Time duration before the request expires [(Go's time.Duration string)](https://golang.org/pkg/time/#Duration.String) | `0s` (no deadline) |
| `--provider` | [Provider](../README.md#enums) | Provider of the bucket | `""` or [default](../README.md#bucket-provider) |
| `--wait` | `bool` | Wait for operation to finish before returning response | `true` |


| Command | Description |
| --- | --- |
| `ais prefetch cloudbucket --provider aws --list 'o1,o2,o3` | Downloads copies of objects o1,o2,o3 from AWS bucket named `cloudbucket` and stores them in the AIS cluster  |

### Rename

`ais rename object BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME`

Rename object from an ais bucket.

#### Examples

1) Rename object `obj1` as `obj2`
```sh
ais rename object mybucket/obj1 obj2
```

### Concat

`ais concat DIRNAME|FILENAME [DIRNAME|FILENAME...] BUCKET/OBJECT_NAME`

Create an object in a bucket by concatenating provided files, keeping the order as in the arguments list.
If directory provided, files within the directory are sorted by filename.
For each file sends a separate request to the cluster.
Supports recursive iteration through directories and wildcards in the same way as PUT operation does.

| Command | Description |
| --- | --- |
| `ais concat file1.txt dir/file2.txt mybucket/obj` | In two separate requests sends `file1.txt` and `dir/file2.txt` to the cluster, concatenates the files keeping the order and saves them as `obj` in bucket `mybucket`  |
| `ais concat file1.txt dir/file2.txt mybucket/obj --verbose` | Same as above, but additionally shows progress bar of sending the files to the cluster  |
| `ais concat dirB dirA mybucket/obj` | Creates `obj` in bucket `mybucket` which is concatenation of sorted files from `dirB` with sorted files from `dirA` |
