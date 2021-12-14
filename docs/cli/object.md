---
layout: post
title: OBJECT
permalink: /docs/cli/object
redirect_from:
 - /cli/object.md/
 - /docs/cli/object.md/
---

# CLI Reference for Objects
This document contains `ais object` commands - the commands to read (GET), write (PUT), APPEND, PROMOTE, PREFETCH, EVICT etc. user data.

## Table of Contents
- [GET object](#get-object)
- [Print object content](#print-object-content)
- [Show object properties](#show-object-properties)
- [PUT object](#put-object)
- [Append file to archive](#append-file-to-archive)
- [Delete object](#delete-object)
- [Evict object](#evict-object)
- [Promote files and directories](#promote-files-and-directories)
- [Move object](#move-object)
- [Concat objects](#concat-objects)
- [Set custom properties](#set-custom-properties)
- [Operations on Lists and Ranges](#operations-on-lists-and-ranges)
  - [Prefetch objects](#prefetch-objects)
  - [Delete multiple objects](#delete-multiple-objects)
  - [Evict multiple objects](#evict-multiple-objects)
  - [Archive multiple objects](#archive-multiple-objects)

# GET object

`ais object get BUCKET/OBJECT_NAME [OUT_FILE]`

Get an object from a bucket.  If a local file of the same name exists, the local file will be overwritten without confirmation.

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |
| `--is-cached` | `bool` | Check if the object is cached locally, without downloading it. | `false` |

`OUT_FILE`: filename in an existing directory or `-` for `stdout`

## Save object to local file with explicit file name

Get the `imagenet_train-000010.tgz` object from the `imagenet` bucket and write it to a local file, `~/train-10.tgz`.

```console
$ ais object get ais://imagenet/imagenet_train-000010.tgz ~/train-10.tgz
GET "imagenet_train-000010.tgz" from bucket "imagenet" as "/home/user/train-10.tgz" [946.8MiB]
```

## Save object to local file with implicit file name

If `OUT_FILE` is omitted, the local file name is implied from the object name.

Get the `imagenet_train-000010.tgz` object from the `imagenet` bucket and write it to a local file, `imagenet_train-000010.tgz`.

```console
$ ais object get imagenet/imagenet_train-000010.tgz
GET "imagenet_train-000010.tgz" from bucket "imagenet" as "imagenet_train-000010.tgz" [946.8MiB]
```

## Get object and print it to standard output

Get the `imagenet_train-000010.tgz` object from the `imagenet` AWS bucket and write it to standard output.

```console
$ ais object get aws://imagenet/imagenet_train-000010.tgz -
```

## Check if object is _cached_

We say that "an object is _cached_" to indicate two separate things:

* The object was originally downloaded from a remote bucket, a bucket in a remote AIS cluster, or a HTTP(s) based dataset;
* The object is stored in the AIS cluster.

In other words, the term "cached" is simply a **shortcut** to indicate the object's immediate availability without the need to go to the object's original location.
Being "cached" does not have any implications on an object's persistence: "cached" objects, similar to those objects that originated in a given AIS cluster, are stored with arbitrary (per bucket configurable) levels of redundancy, etc. In short, the same storage policies apply to "cached" and "non-cached".

The following example checks whether `imagenet_train-000010.tgz` is "cached" in the bucket `imagenet`:

```console
$ ais object get --is-cached ais://imagenet/imagenet_train-000010.tgz
Cached: true
```

### Read range

Get the contents of object `list.txt` from `texts` bucket starting from offset `1024` length `1024` and save it as `~/list.txt` file.

```console
$ ais object get --offset 1024 --length 1024 ais://texts/list.txt ~/list.txt
Read 1.00KiB (1024 B)
```

# Print object content

`ais object cat BUCKET/OBJECT_NAME`

Get `OBJECT_NAME` from bucket `BUCKET` and print it to standard output.
Alias for `ais object get BUCKET/OBJECT_NAME -`.

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |

## Print content of object

Print content of `list.txt` from local bucket `texts` to the standard output.

```console
$ ais object cat ais://texts/list.txt
```

## Read range

Print content of object `list.txt` starting from offset `1024` length `1024` to the standard output.

```console
$ ais object cat ais://texts/list.txt --offset 1024 --length 1024
```

# Show object properties

`ais object show [--props PROP_LIST] BUCKET/OBJECT_NAME`

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
- `ec` - object's EC info (empty if EC is disabled for the bucket, if EC is enabled it looks like `DATA:PARITY[MODE]`, where `DATA` - the number of data slices,
      `PARITY` - the number of parity slices, and `MODE` is protection mode selected for the object: `replicated` - object has `PARITY` replicas on other targets,
      `encoded`  the object is erasure coded and other targets contains only encoded slices

> Note: Like many other `ais show` commands, `ais show object` is aliased to `ais object show` for ease of use.
> Both of these commands are used interchangeably throughout the documentation.

## Show default object properties

Display default properties of object `list.txt` from bucket `texts`.

```console
$ ais object show ais://texts/list.txt
PROPERTY    VALUE
checksum    2d61e9b8b299c41f
size        7.63MiB
atime       06 Jan 20 14:55 PST
version     1
```

## Show all object properties

Display all properties of object `list.txt` from bucket `texts`.

```console
$ ais object show ais://texts/list.txt --props=all
PROPERTY    VALUE
name        provider://texts/list.txt
checksum    2d61e9b8b299c41f
size        7.63MiB
atime       06 Jan 20 14:55 PST
version     2
cached      yes
copies      1
ec          1:1[replicated]
```

## Show selected object properties

Show only selected (`size,version,ec`) properties.

```console
$ ais object show --props size,version,ec ais://texts/listx.txt
PROPERTY    VALUE
size        7.63MiB
version     1
ec          2:2[replicated]
```

# PUT object

`ais object put -|FILE|DIRECTORY BUCKET/[OBJECT_NAME]`<sup>[1](#ft1)</sup>

Put a file, an entire directory of files, or content from STDIN (`-`) into the specified bucket. If an object of the same name exists,
the object will be overwritten without confirmation.

If CLI detects that a user is going to put more than one file, it calculates the total number of files, total data size and checks if the bucket is empty,
then shows all gathered info to the user and asks for confirmation to continue. Confirmation request can be disabled with the option `--yes` for use in scripts.

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose, -v` | `bool` | Enable printing the result of every PUT | `false` |
| `--yes, -y` | `bool` | Answer `yes` to every confirmation prompt | `false` |
| `--conc` | `int` | Number of concurrent `PUT` requests limit | `10` |
| `--recursive, -r` | `bool` | Enable recursive directory upload | `false` |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds). Zero value stops periodical refresh. | `0` if verbose mode is on, `5s` otherwise |
| `--dry-run` | `bool` | Do not actually perform PUT. Shows a few files to be uploaded and corresponding object names for used arguments |
| `--progress` | `bool` | Displays progress bar. Together with `--verbose` shows upload progress for every single file | `false` |
| `--chunk-size` | `string` | Chunk size used for each request, can contain prefix 'b', 'KiB', 'MB' (only applicable when reading from STDIN) | `10MB` |

<a name="ft1">1</a> `FILE|DIRECTORY` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards.
 Symbols `*` and `?` can be used only in a file name pattern. Directory names cannot include wildcards. Only a file name is matched, not full file path, so `/home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory.
 This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `/home/user/img-set-*/*.tar` or `/home/user/bck/**/*.tar.gz`

`FILE` must point to an existing file.
File masks and directory uploading are not supported in single-file upload mode.

## Object names

PUT command handles two possible ways to specify resulting object name if source references single file:
- Object name is not provided: `ais object put path/to/(..)/file.go bucket/` creates object `file.go` in `bucket`
- Explicit object name is provided: `ais object put path/to/(..)/file.go bucket/path/to/object.go` creates object `path/to/object.go` in `bucket`

PUT command handles object naming with range syntax as follows:
- Object names are file paths without longest common prefix of all files from source.
It means that leading part of file path until the last `/` before first `{` is excluded from object name.
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

PUT command handles object naming if its source references directories:
- For path `p` of source directory, resulting objects names are path to files with trimmed `p` prefix
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

All **examples** below put into an empty bucket and the source directory structure is:

```
/home/user/bck/img1.tar
/home/user/bck/img2.zip
/home/user/bck/extra/img1.tar
/home/user/bck/extra/img3.zip
```

The current user HOME directory is `/home/user`.

## Put single file

Put a single file `img1.tar` into local bucket `mybucket`, name it `img-set-1.tar`.

```console
$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

## Put single file with checksum

Put a single file `img1.tar` into local bucket `mybucket`, with a content checksum flag
to override the default bucket checksum performed at the server side.


```console
$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --crc32c 0767345f
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --md5 e91753513c7fc873467c1f3ca061fa70
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --sha256 dc2bac3ba773b7bc52c20aa85e6ce3ae097dec870e7b9bda03671a1c434b7a5d
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --sha512 e7da5269d4cd882deb8d7b7ca5cbf424047f56815fd7723123482e2931823a68d866627a449a55ca3a18f9c9ba7c8bb6219a028ba3ff5a5e905240907d087e40
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar

$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --xxhash 05967d5390ac53b0
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

Optionally, the user can choose to provide a `--compute-cksum` flag for the checksum flag and
let the api take care of the computation.

```console
$ ais object put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --compute-cksum
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

## Put single file without explicit name

Put a single file `~/bck/img1.tar` into bucket `mybucket`, without explicit name.

```console
$ ais object put "~/bck/img1.tar" ais://mybucket/
# PUT /home/user/bck/img1.tar => mybucket/img-set-1.tar
```

## Put content from STDIN

Read unpacked content from STDIN and put it into bucket `mybucket` with name `img-unpacked`.

Note that content is put in chunks what can have a slight overhead.
`--chunk-size` allows for controlling the chunk size - the bigger the chunk size the better performance (but also higher memory usage).

```bash
$ tar -xOzf ~/bck/img1.tar | ais object put - ais://mybucket/img1-unpacked
# PUT /home/user/bck/img1.tar (as stdin) => ais://mybucket/img-unpacked
```

## Put directory into bucket

Put two objects, `/home/user/bck/img1.tar` and `/home/user/bck/img2.zip`, into the root of bucket `mybucket`.
Note that the path `/home/user/bck` is a shortcut for `/home/user/bck/*` and that recursion is disabled by default.

```console
$ ais object put "/home/user/bck" ais://mybucket
# PUT /home/user/bck/img1.tar => img1.tar
# PUT /home/user/bck/img2.tar => img2.zip
```

## Put directory into bucket with directory prefix

The same as above, but add `OBJECT_NAME` (`../subdir/`) prefix to objects names.

```console
$ ais object put "/home/user/bck" ais://mybucket/subdir/
# PUT /home/user/bck/img1.tar => ais://mybucket/subdir/img1.tar
# PUT /home/user/bck/img2.tar => ais://mybucket/subdir/img2.zip
# PUT /home/user/bck/extra/img1.tar => ais://mybucket/subdir/extra/img1.tar
# PUT /home/user/bck/extra/img3.zip => ais://mybucket/subdir/extra/img3.zip
```

## Put directory into bucket with name prefix

The same as above, but without trailing `/`.

```console
$ ais object put "/home/user/bck" ais://mybucket/subdir
# PUT /home/user/bck/img1.tar => ais://mybucket/subdirimg1.tar
# PUT /home/user/bck/img2.tar => ais://mybucket/subdirimg2.zip
# PUT /home/user/bck/extra/img1.tar => ais://mybucket/subdirextra/img1.tar
# PUT /home/user/bck/extra/img3.zip => ais://mybucket/subdirextra/img3.zip
```

## Put files from directory matching pattern

Same as above, except that only files matching pattern `*.tar` are PUT, so the final bucket content is `tars/img1.tar` and `tars/extra/img1.tar`.

```console
$ ais object put "~/bck/*.tar" ais://mybucket/tars/
# PUT /home/user/bck/img1.tar => ais://mybucket/tars/img1.tar
# PUT /home/user/bck/extra/img1.tar => ais://mybucket/tars/extra/img1.tar
```

## Put files with range

Put 9 files to `mybucket` using a range request. Note the formatting of object names.
They exclude the longest parent directory of path which doesn't contain a template (`{a..b}`).

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done

# NOTE: make sure to use double or sinle quotes around the range
$ ais object put "~/dir/test{0..2}{0..2}.txt" ais://mybucket -y
9 objects put into "ais://mybucket" bucket
```

## Put files with range and custom prefix

Same as above, except object names have additional prefix `test${d1}${d2}.txt`.

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done

$ ais object put "~/dir/test{0..2}{0..2}.txt" ais://mybucket/dir/ -y
9 objects put into "ais://mybucket" bucket
# PUT /home/user/dir/test00.txt => ais://mybucket/dir/test00.txt and 8 more
```

## Preview putting files with dry-run

Preview the files that would be sent to the cluster, without really putting them.

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; mkdir -p ~/dir/test${d1}/dir && do echo "0" > ~/dir/test${d1}/dir/test${d2}.txt; done; done
$ ais object put "~/dir/test{0..2}/dir/test{0..2}.txt" ais://mybucket --dry-run
[DRY RUN] No modifications on the cluster
/home/user/dir/test0/dir/test0.txt => ais://mybucket/test0/dir/test0.txt
(...)
```

## PUT multiple directories

Put multiple directories into the cluster with range syntax.

```bash
$ for d1 in {0..10}; do mkdir dir$d1 && for d2 in {0..2}; do echo "0" > dir$d1/test${d2}.txt; done; done
$ ais object put "dir{0..10}" ais://mybucket -y
33 objects put into "ais://mybucket" bucket
# PUT "/home/user/dir0/test0.txt" => b/dir0/test0.txt and 32 more
```

# Append file to archive

`ais object put FILE BUCKET/OBJECT_NAME --archpath ARCH_PATH`

Append a file to an existing archive in a specified bucket. More exactly,
the command allows to append any reader (e.g., an open file) to an existing object formatted as
one of the supported archives.

Environment variable `ARCH_PATH` defines the path inside the archive for the new file.

## Examples

Add a file to an archive

```console
$ # list archived content prior to appending new files
$ ais ls ais://bck --prefix test --list-archive
NAME                             SIZE
test.tar                         42.00KiB
    test.tar/main.c              40.00KiB

$ # add a new file to the archive
$ ais put readme.txt ais://bck/test.tar --archpath=doc/README
APPEND "readme.txt" to object "ais://abc/test.tar[/doc/README]"

$ # check that the archive is updated
$ ais ls ais://bck --prefix test --list-archive
NAME                             SIZE
test.tar                         45.50KiB
    test.tar/doc/README          3.11KiB
    test.tar/main.c              40.00KiB
```

# Promote files and directories

`ais object promote FILE|DIRECTORY BUCKET/[OBJECT_NAME]`<sup>[1](#ft1)</sup>

Promote **AIS-colocated** files and directories to AIS objects in a specified bucket.
Colocation in the context means that the files in question are already located *inside* AIStore (bare-metal or virtual) storage servers (targets).

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose` or `-v` | `bool` | Verbose printout | `false` |
| `--target` | `string` | Target ID; if specified, only the file/dir content stored on the corresponding AIS target is promoted | `""` |
| `--recursive` or `-r` | `bool` | Promote nested directories | `false` |
| `--overwrite` or `-o` | `bool` | Overwrite destination (object) if exists | `true` |
| `--keep` | `bool` | Keep original files | `true` |

**Note:** `--keep` flag defaults to `true` and we retain the origin file to ensure safety.

## Object names

When the specified source references a directory, or a tree of nested directories, object naming is done as follows:

- For path `p` of source directory, resulting objects names are path to files with trimmed `p` prefix
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

If the source references a single file, the resulting object name is set as follows:

- Object name is not provided: `ais object promote /path/to/(..)/file.go ais://bucket/` promotes to object `file.go` in `bucket`
- Explicit object name is provided: `ais object promote /path/to/(..)/file.go ais://bucket/path/to/object.go` promotes object `path/to/object.go` in `bucket`

Notice that `keep` option is required - it cannot be omitted.

> The usual argument for **not keeping** the original file-based content (`keep=false`) is a) saving space on the target servers and b) optimizing time to promote (larger) files and directories.

## Promote a single file

Promote `/tmp/examples/example1.txt` without specified object name.

```console
$ ais object promote /tmp/examples/example1.txt ais://mybucket --keep=true
# PROMOTE /tmp/examples/example1.txt => ais://mybucket/example1.txt
```

## Promote file while specifying custom (resulting) name

Promote /tmp/examples/example1.txt as object with name `example1.txt`.

```console
$ ais object promote /tmp/examples/example1.txt ais://mybucket/example1.txt --keep=true
# PROMOTE /tmp/examples/example1.txt => ais://mybucket/example1.txt
```

## Promote a directory

Make AIS objects out of `/tmp/examples` files (**one file = one object**).
`/tmp/examples` is a directory present on some (or all) of the deployed storage nodes.

```console
$ ais object promote /tmp/examples ais://mybucket/ -r --keep=true
```

## Promote directory with custom prefix

Promote `/tmp/examples` files to AIS objects. Objects names will have `examples/` prefix.

```console
$ ais object promote /tmp/examples ais://mybucket/examples/ -r --keep=false
```

## Promote invalid path

Try to promote a file that does not exist.

```console
$ ais bucket create ais://testbucket
"ais://testbucket" bucket created
$ ais show cluster
TARGET          MEM USED %  MEM AVAIL   CAP USED %  CAP AVAIL   CPU USED %  REBALANCE
1014646t8081    0.00%	    4.00GiB	59%         375.026GiB  0.00%	    finished
...
$ ais object promote /target/1014646t8081/nonexistent/dir/ ais://testbucket --target 1014646t8081 --keep=false
(...) Bad Request: stat /target/1014646t8081/nonexistent/dir: no such file or directory
```

# Delete object

`ais object rm BUCKET/[OBJECT_NAME]...`

Delete an object or list/range of objects from the bucket.

* For multi-object delete operation, please see: [Operations on Lists and Ranges](#operations-on-lists-and-ranges) below.

## Delete a single object

Delete object `myobj.tgz` from bucket `mybucket`.

```console
$ ais object rm ais://mybucket/myobj.tgz
myobj.tgz deleted from ais://mybucket bucket
```

## Delete multiple space-separated objects

Delete objects (`obj1`, `obj2`) from buckets (`aisbck`, `cloudbck`) respectively.

```console
$ ais object rm ais://aisbck/obj1.tgz aws://cloudbck/obj2.tgz
obj1.tgz deleted from ais://aisbck bucket
obj2.tgz deleted from aws://cloudbck bucket
```

* NOTE: for each space-separated object name CLI sends a separate request.
* For multi-object delete that operates on a `--list` or `--template`, please see: [Operations on Lists and Ranges](#operations-on-lists-and-ranges) below.

# Evict object

`ais bucket evict BUCKET/[OBJECT_NAME]...`

[Evict](/docs/bucket.md#prefetchevict-objects) object(s) from a bucket that has [remote backend](/docs/bucket.md).

* NOTE: for each space-separated object name CLI sends a separate request.
* For multi-object eviction that operates on a `--list` or `--template`, please see: [Operations on Lists and Ranges](#operations-on-lists-and-ranges) below.

## Evict a single object

Put `file.txt` object to `cloudbucket` bucket and evict it locally.

```console
$ ais object put file.txt aws://cloudbucket/file.txt
PUT file.txt into bucket aws://cloudbucket

$ ais bucket summary aws://cloudbucket --cached # show only cloudbucket objects present in the AIS cluster
NAME	           OBJECTS	 SIZE    USED %
aws://cloudbucket  1             702B    0%

$ ais bucket evict aws://cloudbucket/file.txt
file.txt evicted from aws://cloudbucket bucket

$ ais bucket summary aws://cloudbucket --cached
NAME	           OBJECTS	 SIZE    USED %
aws://cloudbucket  0             0B      0%
```

## Evict a range of objects

```console
$ ais bucket evict aws://cloudbucket --template "shard-{900..999}.tar"
```

# Move object

`ais object mv BUCKET/OBJECT_NAME NEW_OBJECT_NAME`

Move (rename) an object within an ais bucket.  Moving objects from one bucket to another bucket is not supported.
If the `NEW_OBJECT_NAME` already exists, it will be overwritten without confirmation.

# Concat objects

`ais object concat DIRNAME|FILENAME [DIRNAME|FILENAME...] BUCKET/OBJECT_NAME`

Create an object in a bucket by concatenating the provided files in the order of the arguments provided.
If an object of the same name exists, the object will be overwritten without confirmation.

If a directory is provided, files within the directory are sent in lexical order of filename to the cluster for concatenation.
Recursive iteration through directories and wildcards is supported in the same way as the  PUT operation.

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--recursive` or `-r` | `bool` | Enable recursive directory upload |
| `--progress` | `bool` | Displays progress bar | `false` |

## Concat two files

In two separate requests sends `file1.txt` and `dir/file2.txt` to the cluster, concatenates the files keeping the order and saves them as `obj` in bucket `mybucket`.

```console
$ ais object concat file1.txt dir/file2.txt ais://mybucket/obj
```

## Concat with progress bar

Same as above, but additionally shows progress bar of sending the files to the cluster.

```console
$ ais object concat file1.txt dir/file2.txt ais://mybucket/obj --progress
```

## Concat files from directories

Creates `obj` in bucket `mybucket` which is concatenation of sorted files from `dirB` with sorted files from `dirA`.

```console
$ ais object concat dirB dirA ais://mybucket/obj
```

# Set custom properties

Generally, AIS objects have two kinds of properties: system and, optionally, custom (user-defined). Unlike the system-maintained properties, such as checksum and the number of copies (or EC parity slices, etc.) custom properties may have arbitrary user-defined names and values.

Custom properties are not impacted by object updates (PUTs) - a new version of an object simply inherits custom properties of the previous version, as is with no changes.

The command's syntax is similar to the one used to assign [bucket properties](bucket.md#set-bucket-properties):

`ais object set-custom [command options] BUCKET/OBJECT_NAME JSON_SPECIFICATION|KEY=VALUE [KEY=VALUE...]`,

for example:

```console
$ ais object put README.md ais://abc
$ ais object set-custom ais://abc/README.md mykey1=value1 mykey2=value2

# or, the same using JSON formatting:
$ ais object set-custom ais://abc/README.md '{"mykey1":"value1", "mykey2":"value2"}'
```

To show the results:

```console
$ ais show object ais://abc/README.md --props=all
PROPERTY         VALUE
atime            30 Jun 21 09:43 PDT
cached           yes
checksum         47904b6991a92ca9
copies           1
custom           mykey1=value1, mykey2=value2
ec               -
name             ais://abc/README.md
size             13.13KiB
version          1
```

Note the flag `--props=all` used to show _all_ object's properties including the custom ones, if available.

# Operations on Lists and Ranges

Generally, multi-object operations are supported in two different ways:

1. specifying source directory in the command line - see e.g. [Promote files and directories](#promote-files-and-directories) and [Concat objects](#concat-objects);
2. via `--list` or `--template` options, whereby the latter supports Bash expansion syntax and can also contain prefix, such as a virtul parent directory, etc.)

This section documents and exemplifies AIS CLI operating on multiple (source) objects that you can specify either explicitly or implicitly
using the `--list` or the `--template` flags.

The number of objects "involved" in a single operation does not have any designed-in limitations: all AIS targets work on a given multi-object operation simultaneously and in parallel.

* **See also:** [List/Range Operations](/docs/batch.md#listrange-operations).

## Prefetch objects

`ais job start prefetch BUCKET/ --list|--template <value>`

[Prefetch](/docs/bucket.md#prefetchevict-objects) objects from a remote bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion | `""` |
| `--template` | `string` | The object name template with optional range parts | `""` |
| `--dry-run` | `bool` | Do not actually perform PREFETCH. Shows a few objects to be prefetched |

Options `--list` and `--template` are mutually exclusive.

### Prefetch a list of objects

NOTE: make sure to use double or single quotations to specify the list, as shown below.

```console
# Prefetch o1, o2, and o3 from AWS bucket `cloudbucket`:
$ ais job start prefetch aws://cloudbucket --list 'o1,o2,o3'
```

### Prefetch a range of objects

```console
# Prefetch from AWS bucket `cloudbucket` all objects in the specified range.
# NOTE: make sure to use double or single quotations to specify the template (aka "range")

$ ais job start prefetch aws://cloudbucket --template "shard-{001..999}.tar"
```

## Delete multiple objects

`ais object rm BUCKET/[OBJECT_NAME]...`

Delete an object or list or range of objects from a bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion | `""` |
| `--template` | `string` | The object name template with optional range parts | `""` |

### Delete a list of objects

Delete a list of objects (`obj1`, `obj2`, `obj3`) from bucket `mybucket`.

NOTE: when specifying a comma-delimited `--list` option, make sure to use double or single quotations as shown below.

```console
$ ais object rm ais://mybucket --list "obj1, obj2, obj3"
[obj1 obj2] removed from ais://mybucket bucket
```

### Delete a range of objects

```console
# Delete from bucket `mybucket` all objects in the range `001-003` with prefix `test-`.
# NOTE: when specifying template (aka "range") make sure to use double or single quotation marks.

$ ais object rm ais://mybucket --template "test-{001..003}"
removed files in the range 'test-{001..003}' from ais://mybucket bucket
```

And one other example (that also includes generating .tar shards):

```console
$ ais advanced gen-shards "ais://dsort-testing/shard-{001..999}.tar" --fcount 256
Shards created: 999/999 [==============================================================] 100 %

# NOTE: make sure to use double or single quotations to specify the template (aka "range")
$ ais object rm ais://dsort-testing --template 'shard-{900..999}.tar'
removed from ais://dsort-testing objects in the range "shard-{900..999}.tar", use 'ais job show xaction EH291ljOy' to monitor progress
```

## Evict multiple objects

`ais bucket evict BUCKET/[OBJECT_NAME]...`

[Evict](/docs/bucket.md#prefetchevict-objects) objects from a remote bucket.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--list` | `string` | Comma separated list of objects for list deletion | `""` |
| `--template` | `string` | The object name template with optional range parts | `""` |
| `--dry-run` | `bool` | Do not actually perform EVICT. Shows a few objects to be evicted |

Note that options `--list` and `--template` are mutually exclusive.

### Evict a range of objects

```console
$ ais bucket evict aws://cloudbucket --template "shard-{900..999}.tar"
```

## Archive multiple objects

This is an archive-**creating** operation that takes in multiple objects from a source bucket and archives them all into a destination bucket, where:

* source and destination buckets may not necessarily be different;
* both `--list` and `--template` options are supported
* supported archival formats include `.tar`, `.tar.gz` (or, same, `.tgz`), and `.zip`; more extensions may be added in the future.
* archiving is carried out asynchronously, in parallel by all AIS targets.

```console
# TAR objects `obj1`, `obj2` , `obj3` in a given (destination) bucket called `destbck`.
# NOTE: when specifying `--list` or `--template`, make sure to use double or single quotation marks.

$ ais object put ais://destbck/myarch.tar --source-bck=ais://abc --list="obj1,  obj2,obj3" --archive
```

```console
# ZIP objects `obj1`, `obj2` , `obj3`. Note that in this example source and destination are identical.

$ ais object put ais://mybck/myarch.zip --source-bck=ais://mybck --list="obj1,obj2, obj3" --archive
```
