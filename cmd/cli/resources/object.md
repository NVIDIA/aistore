---
layout: post
title: OBJECT
permalink: cmd/cli/resources/object
redirect_from:
 - cmd/cli/resources/object.md/
---

# GET, PUT, APPEND, PROMOTE, and other operations on objects

- [Get object](#get-object)
- [Print object content](#print-object-content)
- [Show object properties](#show-object-properties)
- [Put object](#put-object)
- [Promote objects](#promote-objects)
- [Delete objects](#delete-objects)
- [Evict objects](#evict-objects)
- [Prefetch objects](#prefetch-objects)
- [Rename object](#rename-object)
- [Concat objects](#concat-objects)

## Get object

`ais get BUCKET_NAME/OBJECT_NAME OUT_FILE`

Get an object from a bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |
| `--is-cached` | `bool` | Check if the object is cached locally, without downloading it. | `false` |

`OUT_FILE`: filename in already existing directory or `-` for `stdout`

### Examples

#### Save object to local file

Get `imagenet_train-000010.tgz` object from `imagenet` bucket and write it as `~/train-10.tgz` file.

```console
$ ais get imagenet/imagenet_train-000010.tgz ~/train-10.tgz
train-10.tgz has the size 946.8MiB (992791757 B)
```

#### Get object and print it to standard output

Get `imagenet_train-000010.tgz` object from `imagenet` cloud bucket and write it to standard output.

```console
$ ais get cloud://imagenet/imagenet_train-000010.tgz -
```

#### Check if object is cached

Check if the object `imagenet_train-000010.tgz` from `imagenet` bucket is cached locally.

```console
$ ais get --is-cached imagenet/imagenet_train-000010.tgz
Cached: true
```

#### Read range

Get contents of object `list.txt` from `texts` bucket starting from offset `1024` length `1024` and save it as `~/list.txt` file.

```console
$ ais get --offset 1024 --length 1024 texts/list.txt ~/list.txt
Read 1.00KiB (1024 B)
```

## Print object content

`ais cat BUCKET_NAME/OBJECT_NAME`

Get `OBJECT_NAME` from bucket `BUCKET_NAME` and print it to standard output.
Alias for `ais get BUCKET_NAME/OBJECT_NAME -`.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |

### Examples

#### Print content of object

Print content of `list.txt` from local bucket `texts` to the standard output.

```console
$ ais cat texts/list.txt
```

#### Read range

Print content of object `list.txt` starting from offset `1024` length `1024` to the standard output.

```console
$ ais cat texts/list.txt --offset 1024 --length 1024
```

## Show object properties

`ais show object [--props PROP_LIST] BUCKET_NAME/OBJECT_NAME`

Get object detailed information.
`PROP_LIST` is a comma-separated list of properties to display.
If `PROP_LIST` is omitted default properties are shown.

Supported properties:

- `cached` - is the object cached on local drives (always `true` for AIS buckets)
- `size` - object size
- `version` - object version (it is empty if versioning is disabled for the bucket)
- `atime` - object's last access time
- `copies` - the number of object replicas per target (empty if bucket mirroring is disabled)
- `checksum` - object's checksum
- `ec` - object's EC info (empty if EC is disabled for the bucket, if EC is enabled it looks like `DATA:PARITY[MODE]`, where `DATA` - the number of data slices, `PARITY` - the number of parity slices, and `MODE` is protection mode selected for the object: `replicated` - object has `PARITY` replicas on other targets, `encoded`  the object is erasure coded and other targets contains only encoded slices

### Examples

#### Show default object properties

Display default properties of object `list.txt` from bucket `texts`.

```console
$ ais show object texts/list.txt
CHECKSUM                SIZE    ATIME                   VERSION
2d61e9b8b299c41f        7.63MiB 06 Jan 20 14:55 PST     1
```

#### Show all object properties

Display all properties of object `list.txt` from bucket `texts`.

```console
$ ais show object texts/list.txt --props=all
CHECKSUM		 SIZE		 ATIME			 VERSION	 CACHED	 COPIES	 EC
2d61e9b8b299c41f         7.63MiB	 06 Jan 20 14:55 PST	 2		 yes	 1	 1:1[replicated]
```

#### Show selected object properties

Show only selected (`size,version,ec`) properties.

```console
$ ais show object --props size,version,ec texts/listx.txt`
SIZE    VERSION EC
7.63MiB 1       2:2[replicated]
```

## Put object

`ais put -|FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup>[1](#ft1)</sup>

Put a file, entire directory (of files) or content from STDIN (`-`) into the specified bucket.
If CLI detects that a user is going to put more than one file, it calculates the total number of files, total data size and checks if the bucket is empty, then shows all gathered info to the user and asks for confirmation to continue.
Confirmation request can be disabled with the option `--yes` for use in scripts.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose, -v` | `bool` | Enable printing the result of every PUT | `false` |
| `--yes, -y` | `bool` | Answer `yes` to every confirmation prompt | `false` |
| `--conc` | `int` | Number of concurrent `PUT` requests limit | `10` |
| `--recursive, -r` | `bool` | Enable recursive directory upload | `false` |
| `--refresh` | `string` | Frequency of the reporting the progress (in milliseconds), may contain multiplicative suffix `s`(second) or `m`(minute). Zero value disables periodical refresh | `0` if verbose mode is on, `5s` otherwise |
| `--dry-run` | `bool` | Do not actually perform PUT. Shows a few files to be uploaded and corresponding object names for used arguments |
| `--progress` | `bool` | Displays progress bar. Together with `--verbose` shows upload progress for every single file | `false` |
| `--chunk-size` | `string` | Chunk size used for each request, can contain prefix 'b', 'KiB', 'MB' (only applicable when reading from STDIN) | `10MB` |

<a name="ft1">1</a> `FILE|DIRECTORY` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards.
 Symbols `*` and `?` can be used only in a file name pattern. Directory names cannot include wildcards. Only a file name is matched, not full file path, so `/home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory.
 This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `/home/user/img-set-*/*.tar` or `/home/user/bck/**/*.tar.gz`

`FILE` must point to an existing file.
File masks and directory uploading are not supported in single-file upload mode.


### Object names

PUT command handles two possible ways to specify resulting object name if source references single file:
- Object name is not provided: `ais put path/to/(..)/file.go bucket/` creates object `file.go` in `bucket`
- Explicit object name is provided: `ais put path/to/(..)/file.go bucket/path/to/object.go` creates object `path/to/object.go` in `bucket`

PUT command handles object naming with range syntax as follows:
- Object names are file paths without longest common prefix of all files from source.
It means that leading part of file path until the last `/` before first `{` is excluded from object name.
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

PUT command handles object naming if its source references directories:
- For path `p` of source directory, resulting objects names are path to files with trimmed `p` prefix
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

### Examples

All examples below put into an empty bucket and the source directory structure is:

```
/home/user/bck/img1.tar
/home/user/bck/img2.zip
/home/user/bck/extra/img1.tar
/home/user/bck/extra/img3.zip
```

The current user HOME directory is `/home/user`.

#### Put single file

Put a single file `img1.tar` into local bucket `mybucket`, name it `img-set-1.tar`.

```bash
$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

#### Put single file with checksum

Put a single file `img1.tar` into local bucket `mybucket`, with a content checksum flag
to override the default bucket checksum performed at the server side.


```bash
$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --crc32c 0767345f
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --md5 e91753513c7fc873467c1f3ca061fa70
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --sha256 dc2bac3ba773b7bc52c20aa85e6ce3ae097dec870e7b9bda03671a1c434b7a5d
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --sha512 e7da5269d4cd882deb8d7b7ca5cbf424047f56815fd7723123482e2931823a68d866627a449a55ca3a18f9c9ba7c8bb6219a028ba3ff5a5e905240907d087e40
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --xxhash 05967d5390ac53b0
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

Optionally, the user can choose to provide a `--compute-cksum` flag for the checksum flag and
let the api take care of the computation.

```bash
$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --compute-cksum
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

#### Put single file without explicit name

Put a single file `~/bck/img1.tar` into bucket `mybucket`, without explicit name.

```bash
$ ais put "~/bck/img1.tar" mybucket/
# PUT /home/user/bck/img1.tar => mybucket/img-set-1.tar
```

#### Put content from STDIN

Read unpacked content from STDIN and put it into local bucket `mybucket` with name `img-unpacked`.

Note that content is put in chunks what can have a slight overhead.
`--chunk-size` allows for controlling the chunk size - the bigger the chunk size the better performance (but also higher memory usage).

```bash
$ tar -xOzf ~/bck/img1.tar | ais put - ais://mybucket/img1-unpacked
# PUT /home/user/bck/img1.tar (as stdin) => ais://mybucket/img-unpacked
```


#### Put directory into bucket

Put two objects, `/home/user/bck/img1.tar` and `/home/user/bck/img2.zip`, into the root of bucket `mybucket`.
Note that the path `/home/user/bck` is a shortcut for `/home/user/bck/*` and that recursion is disabled by default.

```bash
$ ais put "/home/user/bck" mybucket
# PUT /home/user/bck/img1.tar => img1.tar
# PUT /home/user/bck/img2.tar => img2.zip
```

#### Put directory into bucket with directory prefix

The same as above, but add `OBJECT_NAME` (`../subdir/`) prefix to objects names.

```bash
$ ais put "/home/user/bck" mybucket/subdir/
# PUT /home/user/bck/img1.tar => mybucket/subdir/img1.tar
# PUT /home/user/bck/img2.tar => mybucket/subdir/img2.zip
# PUT /home/user/bck/extra/img1.tar => mybucket/subdir/extra/img1.tar
# PUT /home/user/bck/extra/img3.zip => mybucket/subdir/extra/img3.zip
```

#### Put directory into bucket with name prefix

The same as above, but without trailing `/`.

```bash
$ ais put "/home/user/bck" mybucket/subdir
# PUT /home/user/bck/img1.tar => mybucket/subdirimg1.tar
# PUT /home/user/bck/img2.tar => mybucket/subdirimg2.zip
# PUT /home/user/bck/extra/img1.tar => mybucket/subdirextra/img1.tar
# PUT /home/user/bck/extra/img3.zip => mybucket/subdirextra/img3.zip
```

#### Put files from directory matching pattern

Same as above, except that only files matching pattern `*.tar` are PUT, so the final bucket content is `tars/img1.tar` and `tars/extra/img1.tar`.

```bash
$ ais put "~/bck/*.tar" mybucket/tars/
# PUT /home/user/bck/img1.tar => mybucket/tars/img1.tar
# PUT /home/user/bck/extra/img1.tar => mybucket/tars/extra/img1.tar
```

#### Put files with range

Put 9 files to `mybucket` using range request. Note the formatting of object names.
They exclude the longest parent directory of path which doesn't contain a template (`{a..b}`).

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done
$ ais put "~/dir/test{0..2}{0..2}.txt" mybucket -y
9 objects put into "mybucket" bucket
# PUT /home/user/dir/test00.txt => mybucket/test00.txt and 8 more
```

#### Put files with range and custom prefix

Same as above, except object names have additional prefix `test${d1}${d2}.txt`.

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done
$ ais put "~/dir/test{0..2}{0..2}.txt" mybucket/dir/ -y
9 objects put into "mybucket" bucket
# PUT /home/user/dir/test00.txt => mybucket/dir/test00.txt and 8 more
```

#### Preview putting files with dry-run

Preview the files that would be sent to the cluster, without really putting them.

```console
$ for d1 in {0..2}; do for d2 in {0..2}; mkdir -p ~/dir/test${d1}/dir && do echo "0" > ~/dir/test${d1}/dir/test${d2}.txt; done; done
$ ais put "~/dir/test{0..2}/dir/test{0..2}.txt" mybucket --dry-run
[DRY RUN] No modifications on the cluster
/home/user/dir/test0/dir/test0.txt => mybucket/test0/dir/test0.txt
(...)
```

#### Put multiple directories

Put multiple directories into the cluster with range syntax.

```bash
$ for d1 in {0..10}; do mkdir dir$d1 && for d2 in {0..2}; do echo "0" > dir$d1/test${d2}.txt; done; done
$ ais put "dir{0..10}" mybucket -y
33 objects put into "mybucket" bucket
# PUT "/home/user/dir0/test0.txt" => b/dir0/test0.txt and 32 more
```

## Promote objects

`ais promote FILE|DIRECTORY BUCKET_NAME/[OBJECT_NAME]`<sup>[1](#ft1)</sup>

Promote **AIS-colocated** files and directories to AIS objects in a specified bucket.
Colocation in the context means that the files in question are already located *inside* AIStore (bare-metal or virtual) storage servers (targets).

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose` or `-v` | `bool` | Verbose printout | `false` |
| `--target` | `string` | Target ID; if specified, only the file/dir content stored on the corresponding AIS target is promoted | `""` |
| `--recursive` or `-r` | `bool` | Promote nested directories | `false` |
| `--overwrite` or `-o` | `bool` | Overwrite destination (object) if exists | `false` |

### Object names

PROMOTE command handles two possible ways to specify resulting object name if source references single file:
- Object name is not provided: `ais promote /path/to/(..)/file.go bucket/` promotes to object `file.go` in `bucket`
- Explicit object name is provided: `ais promote /path/to/(..)/file.go bucket/path/to/object.go` promotes object `path/to/object.go` in `bucket`

PROMOTE command handles object naming if its source references directories:
- For path `p` of source directory, resulting objects names are path to files with trimmed `p` prefix
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

### Examples

#### Promote single file

Promote `/tmp/examples/example1.txt` without specified object name.

```bash
$ ais promote /tmp/examples/example1.txt mybucket
# PROMOTE /tmp/examples/example1.txt => mybucket/example1.txt
```

#### Promote file with specifying custom name

Promote /tmp/examples/example1.txt as object with name `example1.txt`.

```bash
$ ais promote /tmp/examples/example1.txt mybucket/example1.txt
# PROMOTE /tmp/examples/example1.txt => mybucket/example1.txt
```

#### Promote directory

Make AIS objects out of `/tmp/examples` files (**one file = one object**).
`/tmp/examples` is a directory present on some (or all) of the deployed storage nodes.

```console
$ ais promote /tmp/examples mybucket/ -r
```

#### Promote directory with specifying custom prefix

Promote `/tmp/examples` files to AIS objects. Objects names will have `examples/` prefix.

```console
$ ais promote /tmp/examples mybucket/examples/ -r
```

#### Promote invalid path

Try to promote a file that does not exist.

```console
$ ais create bucket testbucket
testbucket bucket created
$ ais show cluster
TARGET		 MEM USED %	 MEM AVAIL	 CAP USED %	 CAP AVAIL	 CPU USED %	 REBALANCE
1014646t8081	   0.00		 4.00GiB	 59		 375.026GiB	   0.00		 finished; 1 moved (2.5KiB)
...
$ ais promote /target/1014646t8081/nonexistent/dir/ testbucket --target 1014646t8081
(...) Bad Request: stat /target/1014646t8081/nonexistent/dir: no such file or directory
```

## Delete objects

`ais rm object BUCKET_NAME/[OBJECT_NAME]...`

Delete an object or list/range of objects from the bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion | `""` |
| `--template` | `string` | The object name template with optional range parts | `""` |

- Options `--list`, `--template`, and argument(s) `OBJECT_NAME` are mutually exclusive
- List and template deletions expect only a bucket name
- If OBJECT_NAMEs are given, CLI sends a separate request for each object

See [List/Range Operations](../../../docs/batch.md#listrange-operation) for more details.

### Examples

#### Delete single object

Delete object `myobj.tgz` from bucket `mybucket`.

```console
$ ais rm object ais://mybucket/myobj.tgz
myobj.tgz deleted from ais://mybucket bucket
```

#### Delete multiple objects

Delete objects (`obj1`, `obj2`) from buckets (`aisbck`, `cloudbck`) respectively.

```console
$ ais rm object aisbck/obj1.tgz cloudbck/obj2.tgz
obj1.tgz deleted from aisbck bucket
obj2.tgz deleted from cloudbck bucket
```

#### Delete list of objects

Delete a list of objects (`obj1`, `obj2`, `obj3`) from bucket `mybucket`.

```console
$ ais rm object mybucket --list "obj1,obj2,obj3"
[obj1 obj2] removed from dsort-testing bucket
```

#### Delete range of objects

Delete all objects in range `001-003`, with prefix `test-`, from bucket `mybucket`.

```console
$ ais rm object mybucket --template "test-{001..003}"
removed files in the range 'test-{001..003}' from mybucket bucket
```

## Evict objects

`ais evict BUCKET_NAME/[OBJECT_NAME]...`

[Evict](../../../docs/bucket.md#prefetchevict-objects) objects from a cloud bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion | `""` |
| `--template` | `string` | The object name template with optional range parts | `""` |
| `--dry-run` | `bool` | Do not actually perform EVICT. Shows a few objects to be evicted |

- Options `--list`, `--template`, and argument(s) `OBJECT_NAME` are mutually exclusive
- List and template evictions expect only a bucket name
- If OBJECT_NAMEs are given, CLI sends a separate request for each object

See [List/Range Operations](../../../docs/batch.md#listrange-operation) for more details.

### Examples

#### Evict single object

Put `file.txt` object to `cloudbucket` bucket and evict it locally.

```console
$ ais put file.txt cloudbucket/file.txt
PUT file.txt into bucket cloudbucket
$ ais show bucket cloudbucket --cached # show only cloudbucket objects present in the AIS cluster
NAME	           OBJECTS	 SIZE    USED %
aws://cloudbucket  1             702B    0%
$ ais evict cloudbucket/file.txt
file.txt evicted from cloudbucket bucket
$ ais show bucket cloudbucket --cached
NAME	           OBJECTS	 SIZE    USED %
aws://cloudbucket  0             0B      0%
```

## Prefetch objects

`ais start prefetch BUCKET_NAME/ --list|--template <value>`

[Prefetch](../../../docs/bucket.md#prefetchevict-objects) objects from the cloud bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion | `""` |
| `--template` | `string` | The object name template with optional range parts | `""` |
| `--dry-run` | `bool` | Do not actually perform PREFETCH. Shows a few objects to be prefetched |

Options `--list` and `--template` are mutually exclusive.

See [List/Range Operations](../../../docs/batch.md#listrange-operations) for more details.

### Examples

#### Prefetch list of objects

Downloads copies of objects o1,o2,o3 from AWS bucket named `cloudbucket` and stores them in the AIS cluster

```console
$ ais start prefetch aws://cloudbucket --list 'o1,o2,o3'
```

## Rename object

`ais rename object BUCKET_NAME/OBJECT_NAME NEW_OBJECT_NAME`

Rename object from an ais bucket.

## Concat objects

`ais concat DIRNAME|FILENAME [DIRNAME|FILENAME...] BUCKET/OBJECT_NAME`

Create an object in a bucket by concatenating provided files, keeping the order as in the arguments list.
If directory provided, files within the directory are sorted by filename.
For each file sends a separate request to the cluster.
Supports recursive iteration through directories and wildcards in the same way as PUT operation does.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--recursive` or `-r` | `bool` | Enable recursive directory upload |
| `--progress` | `bool` | Displays progress bar | `false` |

### Examples

#### Concat two files

In two separate requests sends `file1.txt` and `dir/file2.txt` to the cluster, concatenates the files keeping the order and saves them as `obj` in bucket `mybucket`.

```console
$ ais concat file1.txt dir/file2.txt mybucket/obj
```

#### Concat with progress bar

Same as above, but additionally shows progress bar of sending the files to the cluster.

```console
$ ais concat file1.txt dir/file2.txt mybucket/obj --progress
```

#### Concat files from directories

Creates `obj` in bucket `mybucket` which is concatenation of sorted files from `dirB` with sorted files from `dirA`.

```console
$ ais concat dirB dirA mybucket/obj
```
