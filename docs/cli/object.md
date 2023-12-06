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
  - [Save object to local file](#save-object-to-local-file)
  - [Save object to local file with implied file name](#save-object-to-local-file-with-implied-file-name)
  - [Get object and print it to standard output](#get-object-and-print-it-to-standard-output)
  - [Check if object is _cached_](#check-if-object-is-cached)
  - [Read range](#read-range)
- [GET multiple objects](#get-multiple-objects)
- [GET archived content](#get-archived-content)
- [Print object content](#print-object-content)
- [Show object properties](#show-object-properties)
- [PUT object](#put-object)
  - [Object names](#object-names)
  - [Put single file](#put-single-file)
  - [Put single file with checksum](#put-single-file-with-checksum)
  - [Put single file with implicitly defined name](#put-single-file-with-implicitly-defined-name)
  - [Put content from STDIN](#put-content-from-stdin)
  - [Put directory](#put-directory)
  - [Put multiple files with prefix added to destination object names](#put-multiple-files-with-prefix-added-to-destination-object-names)
  - [PUT multiple files into virtual directory, track progress](#put-multiple-files-into-virtual-directory-track-progress)
  - [Put pattern-matching files from directory](#put-pattern-matching-files-from-directory)
  - [Put a range of files](#put-a-range-of-files)
  - [Put a list of files](#put-a-list-of-files)
  - [Dry-Run option](#dry-run-option)
  - [Put multiple directories](#put-multiple-directories)
  - [Put multiple directories with the `--skip-vc` option](#put-multiple-directories-with-the-skip-vc-option)
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

# GET object

The command is very useful in terms getting your data out of the cluster. In its brief description:

`ais get [command options] BUCKET[/OBJECT_NAME] [OUT_FILE|-]`

there's

* a bucket source with an optional object name (`BUCKET[/OBJECT_NAME]`), and
* destination (but also optional) `[OUT_FILE]` or standard output (`-`)

Here's in detail:

```console
$ ais get --help

   ais get - (alias for "object get") get an object, a shard, an archived file, or a range of bytes from all of the above;
              write the content locally with destination options including: filename, directory, STDOUT ('-'), or '/dev/null' (discard);
              assorted options further include:
              - '--prefix' to get multiple objects in one shot (empty prefix for the entire bucket);
              - '--extract' or '--archpath' to extract archived content;
              - '--progress' and '--refresh' to watch progress bar;
              - '-v' to produce verbose output when getting multiple objects.
USAGE:
   ais get [command options] BUCKET[/OBJECT_NAME] [OUT_FILE|OUT_DIR|-]

OPTIONS:
   --offset value    object read offset; must be used together with '--length'; default formatting: IEC (use '--units' to override)
   --length value    object read length; default formatting: IEC (use '--units' to override)
   --checksum        validate checksum
   --yes, -y         assume 'yes' to all questions
   --check-cached    check if a given object from a remote bucket is present ("cached") in AIS
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --progress        show progress bar(s) and progress of execution in real time
   --archpath value  extract the specified file from an archive (shard)
   --extract, -x     extract all files from archive(s)
   --prefix value    get objects that start with the specified prefix, e.g.:
                     '--prefix a/b/c' - get objects from the virtual directory a/b/c and objects from the virtual directory
                     a/b that have their names (relative to this directory) starting with c;
                     '--prefix ""' - get entire bucket
   --cached          get only those objects from a remote bucket that are present ("cached") in AIS
   --archive         list archived content (see docs/archive.md for details)
   --limit value     limit object name count (0 - unlimited) (default: 0)
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --verbose, -v     verbose outout when getting multiple objects
   --help, -h        show help
```

## Save object to local file

Get the `imagenet_train-000010.tgz` object from the `imagenet` bucket and write it to a local file, `~/train-10.tgz`:

```console
$ ais get ais://imagenet/imagenet_train-000010.tgz ~/train-10.tgz
GET "imagenet_train-000010.tgz" from bucket "imagenet" as "/home/user/train-10.tgz" [946.8MiB]
```

For comparison, the same GET using `curl` and [the two supported variants of RESTful API](/docs/http_api.md):

```console
# 1. curl GET using conventional RESTful API
# (`aistore` in the URL is a host that runs any AIStore gateway that can be specified via `AIS_ENDPOINT` environment):

$ curl -L -X GET 'http://aistore/v1/objects/imagenet/magenet_train-000010.tgz?provider=gs -o ~/train-10.tgz'

# 2. and the same using "easy URL":

$ curl -L -X GET 'http://aistore/ais/imagenet/magenet_train-000010.tgz -o ~/train-10.tgz'
```

## Save object to local file with implied file name

If `OUT_FILE` is omitted, the local file name is implied from the object name.

Get the `imagenet_train-000010.tgz` object from the `imagenet` bucket and write it to a local file, `imagenet_train-000010.tgz`:

```console
$ ais get imagenet/imagenet_train-000010.tgz
GET "imagenet_train-000010.tgz" from bucket "imagenet" as "imagenet_train-000010.tgz" [946.8MiB]
```

## Get object and print it to standard output

Get the `imagenet_train-000010.tgz` object from the `imagenet` AWS bucket and write it to standard output:

```console
$ ais get aws://imagenet/imagenet_train-000010.tgz -
```

## Check if object is _cached_

We say that "an object is _cached_" to indicate two separate things:

* The object was originally downloaded from a remote (e.g., 3rd party Cloud) bucket, a bucket in a remote AIS cluster, or a HTTP(s) based dataset;
* The object is stored in the AIS cluster.

In other words, the term "cached" is simply a **shortcut** to indicate the object's immediate availability without the need to go to the object's original location.
Being "cached" does not have any implications on an object's persistence: "cached" objects, similar to objects that originated in a given AIS cluster, are stored with arbitrary (per bucket configurable) levels of redundancy, etc. In short, the same storage policies apply to "cached" and "non-cached".

The following example checks whether `imagenet_train-000010.tgz` is "cached" in the bucket `imagenet`:

```console
$ ais get --cached ais://imagenet/imagenet_train-000010.tgz
Cached: true
```

## Read range

Get the contents of object `list.txt` from `texts` bucket starting from offset `1024` length `1024` and save it as `~/list.txt` file:

```console
$ ais get --offset 1024 --length 1024 ais://texts/list.txt ~/list.txt
Read 1.00KiB (1024 B)
```

### Example: read-range multiple objects

Let's say, bucket ais://src contains 4 copies of [aistore readme](https://github.com/NVIDIA/aistore/blob/master/README.md) in its virtual directory `docs/`:

The following reads 10 bytes from each copy and prints the result:

```console
$ ais get ais://src --prefix "docs/" --offset 0 --length 10 -
Read range 4 objects from ais://src to standard output (total size 50.23KiB) [Y/N]: y

**AIStore **AIStore **AIStore **AIStore $
```

Same as above with automatic confirmation and writing results to `/tmp/w`:

```console
$ ais get ais://src --prefix "docs/" --offset 0 --length 10 /tmp/w -y

$ ls -al /tmp/w | awk '{print $5,$9}'

10 README.md
10 copy1.md
10 copy2.md
10 copy3.md
```

# GET multiple objects

Note that destination in this case is a local directory and that (an empty) prefix indicates getting entire bucket; see `--help` for details.

```console
$ ais get s3://abc /tmp/w --prefix "" --progress
GET 60 objects from s3://abc to /tmp/w (size 92.47MiB) [Y/N]: y
Objects:                     59/60 [============================================================>-] 98 %
Total size:  63.00 MiB / 92.47 MiB [=========================================>--------------------] 68 %
```

# GET archived content

For objects formatted as (.tar, .tar.gz, .tar.lz4, or .zip), it is possible to GET and extract them in one shot. There are two "responsible" options:

| Name | Description |
| --- | --- |
| `--archpath` | extract the specified file from an archive (shard) |
| `--extract` | extract all files from archive(s) |

Maybe the most basic:

### Example: extracting one file using its fully-qualified name::

```console
$ ais get ais://nnn/A.tar/tutorials/README.md /tmp/out --archive
```

> assuming, ais://nnn/A.tar was previously created via (e.g.) `ais archive put docs ais://nnn/A.tar -r`

## Example: extract all files from all shards with a given prefix

Let's say, there's a bucket `ais://dst` with a virtual directory `abc/` that in turn contains:

```console
$ ais ls ais://dst --prefix abc/
NAME             SIZE
abc/A.tar.gz         5.18KiB
abc/B.tar.lz4        247.88KiB
abc/C.tar.zip        4.15KiB
abc/D.tar            2.00KiB
```

Next, we GET and extract them all in the respective sub-directories (note also the `--verbose` option):

```console
$ ais get ais://dst /tmp/w --prefix "abc/" --extract -v

GET 4 objects from ais://dst to /tmp/w (total size 259.21KiB) [Y/N]: y
GET D.tar from ais://dst as "/tmp/w/D.tar" (2.00KiB) and extract as /tmp/w/D
GET A.tar.gz from ais://dst as "/tmp/w/A.tar.gz" (5.18KiB) and extract as /tmp/w/A
GET C.tar.zip from ais://dst as "/tmp/w/C.tar.zip" (4.15KiB) and extract as /tmp/w/C
GET B.tar.lz4 from ais://dst as "/tmp/w/B.tar.lz4" (247.88KiB) and extract as /tmp/w/B
```

### Example: use '--prefix' that crosses shard boundary

For starters, we recursively archive all aistore docs:

```console
$ ais put docs ais://A.tar --archive -r
```

To list a virtual subdirectory _inside_ this newly created shard (e.g.):

```console
$ ais archive ls ais://nnn --prefix A.tar/tutorials
NAME                                             SIZE
    A.tar/tutorials/README.md                    561B
    A.tar/tutorials/etl/compute_md5.md           8.28KiB
    A.tar/tutorials/etl/etl_imagenet_pytorch.md  4.16KiB
    A.tar/tutorials/etl/etl_webdataset.md        3.97KiB
    A.tar/tutorials/various/hdfs_backend.md      5.39KiB
Listed: 5 names
```

Now, extract matching files _from_ the bucket to /tmp/out:

```console
$ ais get ais://nnn --prefix A.tar/tutorials /tmp/out --archive
GET 6 objects from ais://nnn/tmp/out (total size 17.81MiB) [Y/N]: y

$ ls -al /tmp/out/tutorials/
total 20
drwxr-x--- 4 root root 4096 May 13 20:05 ./
drwxr-xr-x 3 root root 4096 May 13 20:05 ../
drwxr-x--- 2 root root 4096 May 13 20:05 etl/
-rw-r--r-- 1 root root  561 May 13 20:05 README.md
drwxr-x--- 2 root root 4096 May 13 20:05 various/
```

The result:
```console
$ tree /tmp/out
/tmp/out
├── A.tar
└── tutorials
    ├── etl
    │   ├── compute_md5.md
    │   ├── etl_imagenet_pytorch.md
    │   └── etl_webdataset.md
    ├── README.md
    └── various
        └── hdfs_backend.md
```

> **NOTE:** for more "archival" options and examples, please see [docs/cli/archive.md](archive.md).

# Print object content

`ais object cat BUCKET/OBJECT_NAME`

Get `OBJECT_NAME` from bucket `BUCKET` and print it to standard output.
Alias for `ais get BUCKET/OBJECT_NAME -`.

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--offset` | `string` | Read offset, which can end with size suffix (k, MB, GiB, ...) | `""` |
| `--length` | `string` | Read length, which can end with size suffix (k, MB, GiB, ...) |  `""` |
| `--checksum` | `bool` | Validate the checksum of the object | `false` |

## Print content of object

Print content of `list.txt` from local bucket `texts` to the standard output:

```console
$ ais object cat ais://texts/list.txt
```

## Read range

Print content of object `list.txt` starting from offset `1024` length `1024` to the standard output:

```console
$ ais object cat ais://texts/list.txt --offset 1024 --length 1024
```

# Show object properties

`ais object show [--props PROP_LIST] BUCKET/OBJECT_NAME`

Get object detailed information.
`PROP_LIST` is a comma-separated list of properties to display.
If `PROP_LIST` is omitted, default properties are shown.

Supported properties:

- `cached` - the object cached on local drives (always `true` for AIS buckets)
- `size` - object size
- `version` - object version (empty if versioning is disabled for the bucket)
- `atime` - object's last access time
- `copies` - the number of object replicas per target (`1` if bucket mirroring is disabled), and mountpath where object and its mirrors are located
- `checksum` - object's checksum
- `node` - on which target the object is located
- `ec` - object's EC info (empty if EC is disabled for the bucket, if EC is enabled it looks like `DATA:PARITY[MODE]`, where `DATA` - the number of data slices,
      `PARITY` - the number of parity slices, and `MODE` is protection mode selected for the object: `replicated` - object has `PARITY` replicas on other targets,
      `encoded`  the object is erasure coded and other targets contains only encoded slices

> `ais object show` is an for `ais object show` - both can be used interchangeably.

## Show default object properties

Display default properties of object `list.txt` from bucket `texts`:

```console
$ ais object show ais://texts/list.txt
PROPERTY    VALUE
checksum    2d61e9b8b299c41f
size        7.63MiB
atime       06 Jan 20 14:55 PST
version     1
```

## Show all object properties

Display all properties of object `list.txt` from bucket `texts`:

```console
$ ais object show ais://texts/list.txt --props=all
PROPERTY    VALUE
atime       06 Jan 20 14:55 PST
checksum    2d61e9b8b299c41f
copies      1 [/data/mp1]
custom      -
ec          1:1[replicated]
name        provider://texts/list.txt
node        t[neft8086]
size        7.63MiB
version     2
```

## Show selected object properties

Show only selected (`size,version,ec`) properties:

```console
$ ais object show --props size,version,ec ais://texts/listx.txt
PROPERTY    VALUE
size        7.63MiB
version     1
ec          2:2[replicated]
```

# PUT object

Briefly:

`ais put [command options] [-|FILE|DIRECTORY[/PATTERN]] BUCKET[/OBJECT_NAME]`<sup>[1](#ft1)</sup>

writes a single file, an entire directory (of files), or a typed content directly from STDIN (`-`) - into the specified (destination) bucket.

Notice the optional `[/PATTERN]` - a regular shell filename-matching primitive  - to select files from the source directory.

If an object of the same name exists, the object will be overwritten without confirmation

> but only if is different, content-wise - writing identical bits is optimized-out

If CLI detects that a user is going to put more than one file, it calculates the total number of files, total data size, and checks if the bucket is empty.

Then it shows all gathered info to the user and asks for confirmation to continue.

Confirmation request can be disabled with the option `--yes` for use in scripts.

When writing from `STDIN`, type Ctrl-D to terminate the input.

## Options

```console
$ ais put --help
NAME:
   ais put - (alias for "object put") PUT or APPEND one file or one directory, or multiple files and/or directories.
              - use optional shell filename pattern (wildcard) to match/select sources;
              - request '--compute-checksum' to facilitate end-to-end protection;
              - progress bar via '--progress' to show runtime execution (uploaded files count and size);
              - when writing directly from standard input use Ctrl-D to terminate;
              - use '--archpath' to APPEND to an existing tar-formatted object.

USAGE:
   ais put [command options] [-|FILE|DIRECTORY[/PATTERN]] BUCKET[/OBJECT_NAME]

OPTIONS:
   --list value        comma-separated list of file names, e.g.:
                       --list 'f1,f2,f3'
                       --list "/home/abc/1.tar, /home/abc/1.cls, /home/abc/1.jpeg"
   --template value    template to match file names; may contain prefix with zero or more ranges (with optional steps and gaps), e.g.:
                       --template '/home/dir/subdir/'
                       --template 'shard-{1000..9999}.tar'
                       --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                       --template "prefix-{0010..9999..2}-suffix"
   --progress          show progress bar(s) and progress of execution in real time
   --refresh value     interval for continuous monitoring;
                       valid time units: ns, us (or µs), ms, s (default), m, h
   --chunk-size value  chunk size in IEC or SI units, or "raw" bytes (e.g.: 1MiB or 1048576; see '--units')
   --conc value        limits number of concurrent put requests and number of concurrent shards created (default: 10)
   --dry-run           preview the results without really running the action
   --recursive, -r     recursive operation
   --verbose, -v       verbose
   --yes, -y           assume 'yes' to all questions
   --include-src-bck   prefix names of archived objects with the source bucket name
   --cont-on-err       keep running archiving xaction in presence of errors in a any given multi-object transaction
   --units value       show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                       iec - IEC format, e.g.: KiB, MiB, GiB (default)
                       si  - SI (metric) format, e.g.: KB, MB, GB
                       raw - do not convert to (or from) human-readable format
   --archpath value    filename in archive
   --archive           archive a given list ('--list') or range ('--template') of objects
   --append-to-arch    add object(s) to an existing (.tar, .tgz, .tar.gz, .zip, .msgpack)-formatted object ("archive", "shard")
   --skip-vc           skip loading object metadata (and the associated checksum & version related processing)
   --compute-checksum  [end-to-end protection] compute client-side checksum configured for the destination bucket
   --crc32c value      compute client-side crc32c checksum
                       and provide it as part of the PUT request for subsequent validation on the server side
   --md5 value         compute client-side md5 checksum
                       and provide it as part of the PUT request for subsequent validation on the server side
   --sha256 value      compute client-side sha256 checksum
                       and provide it as part of the PUT request for subsequent validation on the server side
   --sha512 value      compute client-side sha512 checksum
                       and provide it as part of the PUT request for subsequent validation on the server side
   --xxhash value      compute client-side xxhash checksum
                       and provide it as part of the PUT request for subsequent validation on the server side
   --help, -h          show help
```

<a name="ft1">1</a> `FILE|DIRECTORY` should point to a file or a directory. Wildcards are supported, but they work a bit differently from shell wildcards.
 Symbols `*` and `?` can be used only in a file name pattern. Directory names cannot include wildcards. Only a file name is matched, not full file path, so `/home/user/*.tar --recursive` matches not only `.tar` files inside `/home/user` but any `.tar` file in any `/home/user/` subdirectory.
 This makes shell wildcards like `**` redundant, and the following patterns won't work in `ais`: `/home/user/img-set-*/*.tar` or `/home/user/bck/**/*.tar.gz`

`FILE` must point to an existing file.
File masks and directory uploading are NOT supported in single-file upload mode.

## Object names

PUT command handles two possible ways to specify resulting object name if source references single file:
1. Object name is not provided: `ais put path/to/(..)/file.go bucket/` creates object `file.go` in `bucket`
2. Explicit object name is provided: `ais put path/to/(..)/file.go bucket/path/to/object.go` creates object `path/to/object.go` in `bucket`

PUT command handles object naming with range syntax as follows:
- Object names are file paths without longest common prefix of all files from source.
This means that the leading part of file path until the last `/` before first `{` is excluded from object name.
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

First, compare two simple examples:

```console
$ ais put README.md ais://nnn/ccc/
PUT "README.md" => ais://nnn/ccc/README.md

$ ais put README.md ais://nnn/ccc
PUT "README.md" => ais://nnn/ccc
```

In other words, a **trailing forward slash** in the destination name is interpreted as a destination directory

> which is what one would expect from something like Bash: `cp README.md /nnn/ccc/`

One other example: put a single file `img1.tar` into local bucket `mybucket`, name it `img-set-1.tar`.

```console
$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

## Put single file with checksum

Put a single file `img1.tar` into local bucket `mybucket`, with a content checksum flag
to override the default bucket checksum performed at the server side.

```console
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
let the API take care of the computation.

```console
$ ais put "/home/user/bck/img1.tar" ais://mybucket/img-set-1.tar --compute-cksum
# PUT /home/user/bck/img1.tar => ais://mybucket/img-set-1.tar
```

## Put single file with implicitly defined name

Put a single file `~/bck/img1.tar` into bucket `mybucket`, without explicit name.

```console
$ ais put "~/bck/img1.tar" ais://mybucket/

# PUT /home/user/bck/img1.tar => mybucket/img-set-1.tar
```

## Put content from STDIN

Read unpacked content from STDIN and put it into bucket `mybucket` with name `img-unpacked`.

Note that content is put in chunks that can have a slight overhead.
`--chunk-size` allows for controlling the chunk size - the bigger the chunk size the better performance (but also higher memory usage).

```bash
$ tar -xOzf ~/bck/img1.tar | ais put - ais://mybucket/img1-unpacked
# PUT /home/user/bck/img1.tar (as stdin) => ais://mybucket/img-unpacked
```

## Put directory

Put two objects, `/home/user/bck/img1.tar` and `/home/user/bck/img2.zip`, into the root of bucket `mybucket`.
Note that the path `/home/user/bck` is a shortcut for `/home/user/bck/*` and that recursion is disabled by default.

```console
$ ais put "/home/user/bck" ais://mybucket

# PUT /home/user/bck/img1.tar => img1.tar
# PUT /home/user/bck/img2.tar => img2.zip
```

Alternatively, to reference source directory we can use relative (`../..`) naming.

Also notice progress bar (the `--progress` flag) and `g*` wildcard that allows to select only the filenames that start with 'g'

```console
$ ais put "../../../../bin/g*" ais://vvv --progress
Files to upload:
EXTENSION        COUNT   SIZE
                 8       99.82MiB
.0               2       46.28MiB
TOTAL           10      146.10MiB
Proceed putting to ais://vvv? [Y/N]: y
Uploaded files progress                   10/10 [==============================================================] 100 %
Uploaded sizes progress 146.10 MiB / 146.10 MiB [==============================================================] 100 %
PUT 10 objects to "ais://vvv"
```

> NOTE double quotes to denote the `"../../../../bin/g*"` source above. With pattern matching, using quotation marks is a MUST. Single quotes can be used as well.

## Put multiple files with prefix added to destination object names

The multi-file source can be: a directory, a comma-separated list, a template-defined range - all of the above.

Examples follow below, but also notice:
* the flexibility in terms specifying source-matching templates, and
* destination prefix - with or without trailing forward slash

### Example 1.

```console
$ ais put ais://nnn/fff --template "/tmp/www/shard-{001..002}.tar"
Warning: 'fff' will be used as the destination name prefix for all files matching '/tmp/www/shard-{001..002}.tar'
Proceed anyway? [Y/N]: y
Files to upload:
EXTENSION        COUNT   SIZE
.tar             2       17.00KiB
TOTAL            2       17.00KiB
PUT 2 files => ais://nnn/fff? [Y/N]: y
Done
$ ais ls ais://nnn
NAME                     SIZE
fffshard-001.tar         8.50KiB
fffshard-002.tar         8.50KiB
```

### Example 2.

Same as above, except now we make sure that destination is a virtual directory (notice trailing forward '/'):

```console
$ ais put ais://nnn/ggg/ --template "/tmp/www/shard-{003..004}.tar"
Files to upload:
EXTENSION        COUNT   SIZE
.tar             2       17.00KiB
TOTAL            2       17.00KiB
PUT 2 files => ais://nnn/ggg/? [Y/N]: y
Done
$ ais ls ais://nnn
NAME                     SIZE
fffshard-001.tar         8.50KiB
fffshard-002.tar         8.50KiB
ggg/shard-003.tar        8.50KiB
ggg/shard-004.tar        8.50KiB
```

### Example 3.

Same as above, with `--template` embedded into the source argument:

```console
$ ais put "/tmp/www/shard-{005..006}.tar"  ais://nnn/hhh/
Files to upload:
EXTENSION        COUNT   SIZE
.tar             2       17.00KiB
TOTAL            2       17.00KiB
PUT 2 files => ais://nnn/hhh/? [Y/N]: y
Done
$ ais ls ais://nnn
NAME                     SIZE
fffshard-001.tar         8.50KiB
fffshard-002.tar         8.50KiB
ggg/shard-003.tar        8.50KiB
ggg/shard-004.tar        8.50KiB
hhh/shard-005.tar        8.50KiB
hhh/shard-006.tar        8.50KiB
```

And finally, we can certainly PUT source directory:

### Example 4.

```console
$ ais put /home/user/bck ais://mybucket/subdir/

# PUT /home/user/bck/img1.tar => ais://mybucket/subdir/img1.tar
# PUT /home/user/bck/img2.tar => ais://mybucket/subdir/img2.zip
# PUT /home/user/bck/extra/img1.tar => ais://mybucket/subdir/extra/img1.tar
# PUT /home/user/bck/extra/img3.zip => ais://mybucket/subdir/extra/img3.zip
```

The same as above, but without trailing `/`.

```console
$ ais put "/home/user/bck" ais://mybucket/subdir

# PUT /home/user/bck/img1.tar => ais://mybucket/subdirimg1.tar
# PUT /home/user/bck/img2.tar => ais://mybucket/subdirimg2.zip
# PUT /home/user/bck/extra/img1.tar => ais://mybucket/subdirextra/img1.tar
# PUT /home/user/bck/extra/img3.zip => ais://mybucket/subdirextra/img3.zip
```

## Put multiple files into virtual directory, track progress

Same as above with source files in double quotes below, and with progress bar:

> List of sources that you want to upload can (a) comprize any number (and any mix) of comma-separated files and/or directories, and (b) must be embedded in double or single quotes.

```console
$ ais put "README.md, LICENSE" ais://aaa/my-virt-dir/ --progress -y
Files to upload:
EXTENSION        COUNT   SIZE
                 1       1.05KiB
.md              1       11.24KiB
TOTAL            2       12.29KiB
Uploaded files:                   2/2 [==============================================================] 100 %
Total size:     12.29 KiB / 12.29 KiB [==============================================================] 100 %
PUT 2 objects (non-recursive) to "ais://aaa"
```

> Note '/' suffix in `my-virt-dir/` above - without trailing filepath separator we would simply get a longer filename (filenames) at the root of the destination bucket.

We can now list them in the bucket `ais://aaa` the way we would list a directory:

```console
$ ais ls ais://aaa --prefix my-virt-dir
NAME                     SIZE
my-virt-dir/LICENSE      1.05KiB
my-virt-dir/README.md    11.24KiB
```

## Put pattern-matching files from directory

Same as above, except that only files matching pattern `*.tar` are PUT, so the final bucket content is `tars/img1.tar` and `tars/extra/img1.tar`.

> NOTE double quotes to denote the source. With pattern matching, using quotation marks is a MUST. Single quotes can be used as well.

```console
$ ais put "~/bck/*.tar" ais://mybucket/tars/
# PUT /home/user/bck/img1.tar => ais://mybucket/tars/img1.tar
# PUT /home/user/bck/extra/img1.tar => ais://mybucket/tars/extra/img1.tar
```

Same as above with progress bar, recursion into nested directories, and matching characters anywhere in the filename:

```console
$ ais put "ais/*_t*" ais://vvv --progress --recursive
Files to upload:
EXTENSION        COUNT   SIZE
.go              43      704.40KiB
TOTAL            43      704.40KiB
PUT 43 files => ais://vvv? [Y/N]: y

Uploaded files progress                   43/43 [==============================================================] 100 %
Uploaded sizes progress 704.40 KiB / 704.40 KiB [==============================================================] 100 %
PUT 43 objects to "ais://vvv"
```

The result will look as follows:
```console
...
test/target_test.go              1.55KiB
test/various_test.go             510B
test/xaction_test.go             2.61KiB
tgtobj_test.go                   5.57KiB
utils_test.go                    1.38KiB
```

## Put a range of files

There are several equivalent ways to PUT a templated range of files:

### Example 1.

Put 9 files to `mybucket` using a range request. Note the formatting of object names.
They exclude the longest parent directory of path which doesn't contain a template (`{a..b}`).

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done

# NOTE: make sure to use double or sinle quotes around the range

$ ais put "~/dir/test{0..2}{0..2}.txt" ais://mybucket -y
9 objects put into "ais://mybucket" bucket
```

### Example 2. PUT a range of files into virtial directory

Same as above but in addition destination object names will have additional prefix `subdir/` (notice the trailing `/`)

In other words, this PUT in affect creates a **virtual directory** inside destination `ais://mybucket`

```bash
# prep test files
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done

$ ais put "~/dir/test{0..2}{0..2}.txt" ais://mybucket/subdir/ -y
```

### Example 3.
Finally, the same exact operation can be accomplished using `--template` option

> `--template` is universally supported to specify a range of files or objects

```console
$ ais put ais://mybucket/dir/ -y --template "~/dir/test{0..2}{0..2}.txt"
```

## Put a list of files

There are several equivalent ways to PUT a _list_ of files:

### Example 1. Notice the double quotes (single quotes can be used as well)

```console
$ ais put "README.md,LICENSE" s3://abc
Files to upload:
EXTENSION        COUNT   SIZE
                 1       1.05KiB
.md              1       11.24KiB
TOTAL            2       12.29KiB
PUT 2 files => s3://abc? [Y/N]: y
```

### Example 2.

Alternatively, the same can be done using the `--list` flag:

> `--list` is universally supported to specify a list of files or objects

```console
$ ais put s3://abc --list "README.md,LICENSE"

Files to upload:
EXTENSION        COUNT   SIZE
                 1       1.05KiB
.md              1       11.24KiB
TOTAL            2       12.29KiB
PUT 2 files => s3://abc? [Y/N]: y
```

### Example 3. PUT a list into virtual directory

The only difference from the two examples above is: **trailing `/` in the destination name**.

```console
$ ais put ais://abc/subdir/ --list 'LICENSE,README.md' -y

$ ais ls ais://abc
NAME                     SIZE
subdir/LICENSE           1.05KiB
subdir/README.md         11.24KiB
```

## Dry-Run option

Preview the files that would be sent to the cluster, without actually putting them.

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; mkdir -p ~/dir/test${d1}/dir && do echo "0" > ~/dir/test${d1}/dir/test${d2}.txt; done; done
$ ais put "~/dir/test{0..2}/dir/test{0..2}.txt" ais://mybucket --dry-run

[DRY RUN] No modifications on the cluster
/home/user/dir/test0/dir/test0.txt => ais://mybucket/test0/dir/test0.txt
(...)
```

## Put multiple directories

Put multiple directories into the cluster with range syntax.

```bash
$ for d1 in {0..10}; do mkdir dir$d1 && for d2 in {0..2}; do echo "0" > dir$d1/test${d2}.txt; done; done
$ ais put "dir{0..10}" ais://mybucket -y
33 objects put into "ais://mybucket" bucket
# PUT "/home/user/dir0/test0.txt" => b/dir0/test0.txt and 32 more
```

## Put multiple directories with the `--skip-vc` option

> The `--skip-vc` option allows AIS to skip loading existing object's metadata to perform metadata-associated processing (such as comparing source and destination checksums, for instance). In certain scenarios (e.g., massive uploading of new files that cannot be present in the bucket) this can help reduce PUT latency.

```bash
$ for d1 in {0..10}; do mkdir dir$d1 && for d2 in {0..2}; do echo "0" > dir$d1/test${d2}.txt; done; done
$ ais put "dir{0..10}" ais://mybucket -y --skip-vc

Files to upload:
EXTENSION        COUNT   SIZE
.txt             33      66B
TOTAL            33      66B
```

# Promote files and directories

`ais object promote FILE|DIRECTORY BUCKET/[OBJECT_NAME]`<sup>[1](#ft1)</sup>

Promote **AIS-colocated** files and directories to AIS objects in a specified bucket.
Colocation in context means that the files in question are already located *inside* AIStore (bare-metal or virtual) storage servers (targets).

## Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose` or `-v` | `bool` | Verbose output | `false` |
| `--target-id` | `string` | Target ID; if specified, only the file/dir content stored on the corresponding AIS target is promoted | `""` |
| `--recursive` or `-r` | `bool` | Promote nested directories | `false` |
| `--overwrite-dst` or `-o` | `bool` | Overwrite destination (object) if exists | `false` |
| `--delete-src` | `bool` | Delete promoted source | `false` |
| `--not-file-share` | `bool` | Each target must act autonomously, skipping file-share auto-detection and promoting the entire source (as seen from _the_ target) | `false` |

## Object names

When the specified source references a **directory, or a tree of nested directories**, object naming is set as follows:

- For path `p` of source directory, resulting objects names are path to files with trimmed `p` prefix
- `OBJECT_NAME` is prepended to each object name.
- Abbreviations in source like `../` are not supported at the moment.

If the source references a **single file**, the resulting object name is set as follows:

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

Promote `/tmp/examples` files to AIS objects. Object names will have `examples/` prefix.

```console
$ ais object promote /tmp/examples ais://mybucket/examples/ -r --keep=false
```

## Promote invalid path

Try to promote a file that does not exist.

```console
$ ais create ais://testbucket
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
$ ais put file.txt aws://cloudbucket/file.txt
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

Generally, AIS objects have two kinds of properties: system and, optionally, custom (user-defined). Unlike the system-maintained properties, such as checksum and the number of copies (or EC parity slices, etc.), custom properties may have arbitrary user-defined names and values.

Custom properties are not impacted by object updates (PUTs) -- a new version of an object simply inherits custom properties of the previous version as is with no changes.

The command's syntax is similar to the one used to assign [bucket properties](bucket.md#set-bucket-properties):

`ais object set-custom [command options] BUCKET/OBJECT_NAME JSON_SPECIFICATION|KEY=VALUE [KEY=VALUE...]`,

for example:

```console
$ ais put README.md ais://abc
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

Generally, multi-object operations are supported in 2 different ways:

1. specifying source directory in the command line - see e.g. [Promote files and directories](#promote-files-and-directories) and [Concat objects](#concat-objects);
2. via `--list` or `--template` options, whereby the latter supports Bash expansion syntax and can also contain prefix, such as a virtual parent directory, etc.)

This section documents and exemplifies AIS CLI operating on multiple (source) objects that you can specify either explicitly or implicitly
using the `--list` or `--template` flags.

The number of objects "involved" in a single operation does not have any designed-in limitations: all AIS targets work on a given multi-object operation simultaneously and in parallel.

* **See also:** [List/Range Operations](/docs/batch.md#listrange-operations).

## Prefetch objects

`ais start prefetch BUCKET/ --list|--template <value>`

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
$ ais start prefetch aws://cloudbucket --list 'o1,o2,o3'
```

### Prefetch a range of objects

```console
# Prefetch from AWS bucket `cloudbucket` all objects in the specified range.
# NOTE: make sure to use double or single quotations to specify the template (aka "range")

$ ais start prefetch aws://cloudbucket --template "shard-{001..999}.tar"
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
$ ais archive gen-shards "ais://dsort-testing/shard-{001..999}.tar" --fcount 256
Shards created: 999/999 [==============================================================] 100 %

# NOTE: make sure to use double or single quotations to specify the template (aka "range")
$ ais object rm ais://dsort-testing --template 'shard-{900..999}.tar'
removed from ais://dsort-testing objects in the range "shard-{900..999}.tar", use 'ais job show xaction EH291ljOy' to monitor the progress
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
