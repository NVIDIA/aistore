# CLI Reference for Objects
This document contains `ais object` commands - the commands to read (GET), write (PUT), APPEND, PROMOTE, PREFETCH, EVICT etc. user data.

Namely:

```console
$ ais object <TAB-TAB>

get     put        cp           etl          set-custom   prefetch     show      cat
ls      promote    archive      concat       rm           evict        mv
```

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
- [Out of band updates](/docs/out_of_band.md)
- [PUT object](#put-object)
  - [Object names](#object-names)
  - [Put with client-side checksumming](#put-with-client-side-checksumming)
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
  - [Put multiple directories using Bash range notation](#put-multiple-directories-using-bash-range-notation)
  - [Put multiple directories using filename-matching pattern (wildcard)](#put-multiple-directories-using-filename-matching-pattern-wildcard)
  - [Put multiple directories with the `--skip-vc` option](#put-multiple-directories-with-the-skip-vc-option)
- [Tips for copying files from Lustre (NFS)](#tips-for-copying-files-from-lustre-nfs)
- [Promote files and directories](#promote-files-and-directories)
- [Multipart upload](#multipart-upload)
  - [Create multipart upload](#create-multipart-upload)
  - [Upload parts](#upload-parts)
  - [Complete multipart upload](#complete-multipart-upload)
  - [Abort multipart upload](#abort-multipart-upload)
- [Append object](#append-object)
- [Delete object](#delete-object)
  - [Disambiguating multi-object operation](#disambiguating-multi-object-operation)
- [Evict one remote bucket, multiple remote buckets, or selected objects in a given remote bucket or buckets](#evict-one-remote-bucket-multiple-remote-buckets-or-selected-objects-in-a-given-remote-bucket-or-buckets)
- [Move object](#move-object)
- [Concat objects](#concat-objects)
- [Set custom properties](#set-custom-properties)
- [Operations on Lists and Ranges (and entire buckets)](#operations-on-lists-and-ranges-and-entire-buckets)
  - [Prefetch objects](#prefetch-objects)
  - [Example prefetching objects](#example-prefetching-objects)
  - [Delete multiple objects](#delete-multiple-objects)
  - [Evict multiple objects](#evict-multiple-objects)
  - [Archive multiple objects](/docs/cli/bucket.md#archive-multiple objects)

# GET object

Use `ais object get` or, same, `ais get` to GET data from aistore. In other words, read data from the cluster and, optionally, save it locally.

`ais get BUCKET[/OBJECT_NAME] [OUT_FILE|-]` [command options]

there's

* a bucket source with an optional object name (`BUCKET[/OBJECT_NAME]`), and
* destination (but also optional) `[OUT_FILE]` or standard output (`-`)

Here's in detail:

```console
$ ais get --help

NAME:
   ais get - (alias for "object get") Get an object, a shard, an archived file, or a range of bytes from all of the above;
              write the content locally with destination options including: filename, directory, STDOUT ('-'), or '/dev/null' (discard);
              assorted options further include:
              - '--prefix' to get multiple objects in one shot (empty prefix for the entire bucket);
              - '--extract' or '--archpath' to extract archived content;
              - '--progress' and '--refresh' to watch progress bar;
              - '-v' to produce verbose output when getting multiple objects.

USAGE:
   ais get BUCKET[/OBJECT_NAME] [OUT_FILE|OUT_DIR|-] [command options]

OPTIONS:
   --archive            List archived content (see docs/archive.md for details)
   --archmime value     Expected format (mime type) of an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                        especially usable for shards with non-standard extensions
   --archmode value     Enumerated "matching mode" that tells aistore how to handle '--archregx', one of:
                          * regexp - general purpose regular expression;
                          * prefix - matching filename starts with;
                          * suffix - matching filename ends with;
                          * substr - matching filename contains;
                          * wdskey - WebDataset key
                        example:
                          given a shard containing (subdir/aaa.jpg, subdir/aaa.json, subdir/bbb.jpg, subdir/bbb.json, ...)
                          and wdskey=subdir/aaa, aistore will match and return (subdir/aaa.jpg, subdir/aaa.json)
   --archpath value     Extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                        see also: '--archregx'
   --archregx value     Specifies prefix, suffix, substring, WebDataset key, _or_ a general-purpose regular expression
                        to select possibly multiple matching archived files from a given shard;
                        is used in combination with '--archmode' ("matching mode") option
   --blob-download      Utilize built-in blob-downloader (and the corresponding alternative datapath) to read very large remote objects
   --cached             Only get in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   --check-cached       Check whether a given named object is present in cluster
                        (applies only to buckets with remote backend)
   --checksum           Validate checksum
   --chunk-size value   Chunk size in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   --extract, -x        Extract all files from archive(s)
   --inv-id value       Bucket inventory ID (optional; by default, we use bucket name as the bucket's inventory ID)
   --inv-name value     Bucket inventory name (optional; system default name is '.inventory')
   --inventory          List objects using _bucket inventory_ (docs/s3compat.md); requires s3:// backend; will provide significant performance
                        boost when used with very large s3 buckets; e.g. usage:
                          1) 'ais ls s3://abc --inventory'
                          2) 'ais ls s3://abc --inventory --paged --prefix=subdir/'
                        (see also: docs/s3compat.md)
   --latest             Check in-cluster metadata and, possibly, GET, download, prefetch, or otherwise copy the latest object version
                        from the associated remote bucket;
                        the option provides operation-level control over object versioning (and version synchronization)
                        without the need to change the corresponding bucket configuration: 'versioning.validate_warm_get';
                        see also:
                          - 'ais show bucket BUCKET versioning'
                          - 'ais bucket props set BUCKET versioning'
                          - 'ais ls --check-versions'
                        supported commands include:
                          - 'ais cp', 'ais prefetch', 'ais get'
   --length value       Object read length; default formatting: IEC (use '--units' to override)
   --limit value        The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                        e.g.:
                        - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                        - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                        - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/-- (default: 0)
   --num-workers value  Number of concurrent blob-downloading workers (readers); system default when omitted or zero (default: 0)
   --offset value       Object read offset; must be used together with '--length'; default formatting: IEC (use '--units' to override)
   --prefix value       Get objects with names starting with the specified prefix, e.g.:
                        '--prefix a/b/c' - get objects from the virtual directory a/b/c and objects from the virtual directory
                        a/b that have their names (relative to this directory) starting with 'c';
                        '--prefix ""' - get entire bucket (all objects)
   --progress           Show progress bar(s) and progress of execution in real time
   --refresh value      Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --silent             Server-side flag, an indication for aistore _not_ to log assorted errors (e.g., HEAD(object) failures)
   --skip-lookup        Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                         1) adding remote bucket to aistore without first checking the bucket's accessibility
                            (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                         2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --units value        Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
                        raw - do not convert to (or from) human-readable format
   --verbose, -v        Verbose output
   --yes, -y            Assume 'yes' to all questions
   --help, -h           Show help
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

Let's say, bucket ais://src contains 4 copies of [aistore readme](https://github.com/NVIDIA/aistore/blob/main/README.md) in its virtual directory `docs/`:

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
Listed: 4 names
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
    └── etl
        └── compute_md5.md
        └── etl_imagenet_pytorch.md
        └── etl_webdataset.md
```

> **NOTE:** for more "archival" options and examples, please see [docs/cli/archive.md](archive.md).

# Print object content

`ais object cat BUCKET/OBJECT_NAME`

Get `OBJECT_NAME` from bucket `BUCKET` and print it to standard output.
Alias for `ais get BUCKET/OBJECT_NAME -`.

## Options

```console
$ ais object cat --help

NAME:
   ais object cat - Print object's content to STDOUT (same as Linux shell 'cat')

USAGE:
   ais object cat BUCKET/OBJECT_NAME [command options]

OPTIONS:
   --archpath value  Extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                     see also: '--archregx'
   --checksum        Validate checksum
   --force, -f       Force execution of the command (caution: advanced usage only)
   --length value    Object read length; default formatting: IEC (use '--units' to override)
   --offset value    Object read offset; must be used together with '--length'; default formatting: IEC (use '--units' to override)
   --help, -h        Show help
```

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

`ais put [-|FILE|DIRECTORY[/PATTERN]] BUCKET[/OBJECT_NAME_or_PREFIX]`<sup>[1](#ft1)</sup> [command options]

writes a single file, an entire directory (of files), or a typed content directly from STDIN (`-`) - into the specified (destination) bucket.

Notice the optional `[/PATTERN]` - a regular shell filename-matching primitive  - to select files from the source directory.

If an object of the same name exists, the object will be overwritten without confirmation

> but only if is different, content-wise - writing identical bits is optimized-out

If CLI detects that a user is going to put more than one file, it calculates the total number of files, total data size, and checks if the bucket is empty.

Then it shows all gathered info to the user and asks for confirmation to continue.

Confirmation request can be disabled with the option `--yes` for use in scripts.

When writing from `STDIN`, type Ctrl-D to terminate the input.

## Inline help

```console
$ ais put --help

NAME:
   ais put - (alias for "object put") PUT or append one file, one directory, or multiple files and/or directories.
   Use optional shell filename PATTERN (wildcard) to match/select multiple sources.
   Destination naming is consistent with 'ais object promote' command, whereby the optional OBJECT_NAME_or_PREFIX
   becomes either a name, a prefix, or a virtual destination directory (if it ends with a forward '/').
   Assorted examples and usage options follow (and see docs/cli/object.md for more):
     - upload matching files: 'ais put "docs/*.md" ais://abc/markdown/'
     - (notice quotation marks and a forward slash after 'markdown/' destination);
     - '--compute-checksum': use '--compute-checksum' to facilitate end-to-end protection;
     - '--progress': progress bar, to show running counts and sizes of uploaded files;
     - Ctrl-D: when writing directly from standard input use Ctrl-D to terminate;
     - '--append' to append (concatenate) files, e.g.: 'ais put docs ais://nnn/all-docs --append';
     - '--dry-run': see the results without making any changes.
     Notes:
     - to write or add files to (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted objects ("shards"), use 'ais archive'

USAGE:
   ais put [-|FILE|DIRECTORY[/PATTERN]] BUCKET[/OBJECT_NAME_or_PREFIX] [command options]

OPTIONS:
   --append             Concatenate files: append a file or multiple files as a new _or_ to an existing object
   --chunk-size value   Chunk size in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   --compute-checksum   Compute client-side checksum - one of the supported checksum types that is currently configured for the destination bucket -
                        and provide it as part of the PUT request for subsequent validation on the server side
                        (see also: "end-to-end protection")
   --cont-on-err        Keep running archiving xaction (job) in presence of errors in a any given multi-object transaction
   --crc32c value       compute client-side crc32c checksum
                        and provide it as part of the PUT request for subsequent validation on the server side
   --dry-run            Preview the results without really running the action
   --include-src-dir    Prefix destination object names with the source directory
   --list value         Comma-separated list of object or file names, e.g.:
                        --list 'o1,o2,o3'
                        --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                        or, when listing files and/or directories:
                        --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --md5 value          compute client-side md5 checksum
                        and provide it as part of the PUT request for subsequent validation on the server side
   --num-workers value  Number of concurrent client-side workers (to execute PUT or append requests);
                        use (-1) to indicate single-threaded serial execution (ie., no workers);
                        any positive value will be adjusted _not_ to exceed twice the number of client CPUs (default: 10)
   --progress           Show progress bar(s) and progress of execution in real time
   --recursive, -r      Recursive operation
   --refresh value      Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --retries value      When failing to PUT retry the operation up to so many times (with increasing timeout if timed out) (default: 1)
   --sha256 value       compute client-side sha256 checksum
                        and provide it as part of the PUT request for subsequent validation on the server side
   --sha512 value       compute client-side sha512 checksum
                        and provide it as part of the PUT request for subsequent validation on the server side
   --skip-lookup        Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                         1) adding remote bucket to aistore without first checking the bucket's accessibility
                            (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                         2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --skip-vc            Skip loading object metadata (and the associated checksum & version related processing)
   --template value     Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                        (with optional steps and gaps), e.g.:
                        --template "" # (an empty or '*' template matches everything)
                        --template 'dir/subdir/'
                        --template 'shard-{1000..9999}.tar'
                        --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                        and similarly, when specifying files and directories:
                        --template '/home/dir/subdir/'
                        --template "/abc/prefix-{0010..9999..2}-suffix"
   --timeout value      Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --units value        Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
   --verbose, -v        Verbose output
   --wait               Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --xxhash value       compute client-side xxhash checksum
                        and provide it as part of the PUT request for subsequent validation on the server side
   --yes, -y            Assume 'yes' to all questions
   --help, -h           Show help
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

## Put with client-side checksumming

> **Motivation**: There's always a motivation to perform faster. One way to achieve this is by avoiding redundant writes of user data. A write operation can effectively become a no-op if the identical data already exists in the cluster. The conventional method to establish such identity is through content checksumming.

In short, here's a CLI write-optimizing trick that utilizes client-side checksumming.

### First PUT:

```console
$ time ais put /tmp/www s3://ais-aa --yes --compute-checksum --recursive

Files to upload:
EXTENSION        COUNT   SIZE
                 27      562.90MiB
.go              1       123B
.prev            1       7.62MiB
.txt             2       10.87KiB
TOTAL            31      570.53MiB
Uploaded 4(12%) objects, 9.6MiB (1%).
Uploaded 8(25%) objects, 55.3MiB (9%).
Uploaded 13(41%) objects, 105.8MiB (18%).
Uploaded 23(74%) objects, 315.7MiB (55%).
Uploaded 29(93%) objects, 449.3MiB (78%).
Uploaded 31(100%) objects, 570.5MiB (100%).

PUT 31 files (one directory, recursively) => s3://ais-aa

real    0m44.895s  <<<<<<<<<<<<<<<<<<<<<< 45s
user    0m0.097s
sys     0m0.355s
```

### Second PUT with no changes at the source:

```console
$ time ais put /tmp/www s3://ais-aa --yes --compute-checksum --recursive

Files to upload:
EXTENSION        COUNT   SIZE
                 27      562.90MiB
.go              1       123B
.prev            1       7.62MiB
.txt             2       10.87KiB
TOTAL            31      570.53MiB

PUT 31 files (one directory, recursively) => s3://ais-aa

real    0m0.136s   <<<<<<<<<<<<<<<<<<<<<<<<<<<< (PUT took no time)
user    0m0.107s
sys     0m0.509s
```

### Adding one file to the source:

```console
$ time ais put /tmp/www s3://ais-aa --yes --compute-checksum --recursive

Files to upload:
EXTENSION        COUNT   SIZE
                 28      563.17MiB
.go              1       123B
.prev            1       7.62MiB
.txt             2       10.87KiB
TOTAL            32      570.80MiB

PUT 32 files (one directory, recursively) => s3://ais-aa

real    0m1.029s   <<<<<<<<<<<<<<<<<< 1s
user    0m0.121s
sys     0m0.588s
```

> **Note:** Ideally, the checksum is provided with PUT API calls. The CLI takes it one step further: if client-side checksumming is requested but the checksum is empty, the CLI computes it automatically. The corresponding overhead must be taken into account when analyzing resulting performance.

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

### Example 2. PUT a range of files into a virtual directory

Same as above but in addition destination object names will have additional prefix `subdir/` (notice the trailing `/`)

In other words, this PUT in affect creates a **virtual directory** inside destination `ais://mybucket`

```bash
# first, prepare test files
$ for d1 in {0..2}; do for d2 in {0..2}; do echo "0" > ~/dir/test${d1}${d2}.txt; done; done
```

Next, PUT:

```console
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

### Example 1

```bash
$ for d1 in {0..2}; do for d2 in {0..2}; mkdir -p ~/dir/test${d1}/dir && do echo "0" > ~/dir/test${d1}/dir/test${d2}.txt; done; done
$ ais put "~/dir/test{0..2}/dir/test{0..2}.txt" ais://mybucket --dry-run

[DRY RUN] No modifications on the cluster
/home/user/dir/test0/dir/test0.txt => ais://mybucket/test0/dir/test0.txt
(...)
```

### Example 2

Generally, the `--template` option combines (an optional) prefix and/or one or more ranges (e.g., bash brace expansions).

In this example, we only use the "prefix" part of the `--template` to specify source directory.

```console
$ ls -l /tmp/w
total 32
-rw-r--r-- 1 root root 14180 Dec 11 18:18 111
-rw-r--r-- 1 root root 14180 Dec 11 18:18 222

$ ais put ais://nnn/fff --template /tmp/w --dry-run
[DRY RUN] with no modifications to the cluster
Warning: 'fff' will be used as the destination name prefix for all files from '/tmp/w' directory
Proceed anyway? [Y/N]: y
Files to upload:
EXTENSION        COUNT   SIZE
                 2       27.70KiB
TOTAL            2       27.70KiB
[DRY RUN] PUT 2 files (one directory, non-recursive) => ais://nnn/fff
PUT /tmp/w/222 -> ais://nnn/fff222
PUT /tmp/w/111 -> ais://nnn/fff111
```

> Note: to PUT files into a virtual destination directory, use trailing '/', e.g.: `ais put ais://nnn/fff/ ...`

## Put multiple directories using Bash range notation

First, let's generate some files and directories (strictly for illustration purposes):

```bash
$ for d1 in {0..10}; do mkdir /tmp/testdir_$d1 && for d2 in {0..2}; do echo "0" > /tmp/testdir_$d1/test${d2}.txt; done; done
```

Next, PUT them all in one shot (notice quotation marks!):

```bash
$ ais put "/tmp/testdir_{0..10}" ais://nnn
Files to upload:
EXTENSION        COUNT   SIZE
.txt             33      66B
TOTAL            33      66B

PUT 33 files (11 directories, non-recursive) => ais://nnn? [Y/N]:
```

Let's now take a look at the result - and observe a PROBLEM:

```console
$ ais ls ais://nnn --summary
NAME             PRESENT         OBJECTS         SIZE (apparent, objects, remote)        USAGE(%)
ais://nnn        yes             3 0             112.01KiB 6B 0B                         0%
```

So Yes, the problem is that by default destination object names are _sourced_ from the source file basenames.

In this examples, we happen to have only **3** basenames: `test0.txt`, `test1.txt`, and `test2.txt`.

The **workaround** is to include respective parent directories in the destination naming:

> As always, see `ais put --help` for usage examples and more options.

```console
$ ais put "/tmp/testdir_{0..10}" ais://nnn --include-src-dir
Files to upload:
EXTENSION        COUNT   SIZE
.txt             33      66B
TOTAL            33      66B

PUT 33 files (11 directories, non-recursive) => ais://nnn? [Y/N]: y
Done

$ ais ls ais://nnn --summary
NAME             PRESENT         OBJECTS         SIZE (apparent, objects, remote)        USAGE(%)
ais://nnn        yes             33 0            320.06KiB 66B 0B                        0%
```

## Put multiple directories using filename-matching pattern (wildcard)

Same as above, but **note**: alternative syntax, which is maybe more conventional:

```bash
$ ais put "/tmp/testdir_*" ais://nnn --include-src-dir
Files to upload:
EXTENSION        COUNT   SIZE
.txt             33      66B
TOTAL            33      66B

PUT 33 files (11 directories, non-recursive) => ais://nnn? [Y/N]:
```

## Put multiple directories with the `--skip-vc` option

> The `--skip-vc` option allows AIS to skip loading existing object's metadata to perform metadata-associated processing (such as comparing source and destination checksums, for instance). In certain scenarios (e.g., massive uploading of new files that cannot be present in the bucket) this can help reduce PUT latency.

```bash
## prepare testing content
$ for d1 in {0..10}; do mkdir /tmp/testdir_$d1 && for d2 in {0..2}; do echo "0" > /tmp/testdir_$d1/test${d2}.txt; done; done

## PUT
$ ais put ""/tmp/testdir_{0..10}"" ais://mybucket -y --skip-vc

Files to upload:
EXTENSION        COUNT   SIZE
.txt             33      66B
TOTAL            33      66B
```

# Tips for Copying Files from Lustre (NFS)

Yes, `ais put` can be used to copy remote files - usage tips follow below. Buf first, disclaimer.

## Disclaimer

> Copying large amounts of data from remote (NFS, SMB) locations is not exactly an exercise for a single client machine. There are alternative designed-in [ways](https://aistore.nvidia.com/blog/2022/03/17/promote), whereby all AIStore nodes _partition_ remote source between themselves and do the copying - in parallel.

Performance-wise, the difference from copying via client (or by client) - is two-fold:

1. many orders of magnitude greater horsepower that AIStore can contribute to the effort, and
2. avoidance of the (client <= NFS) and (client => AIStore) roundtrips.

Needless to say, _promoting_ files to objects, as it were, requires that all AIS nodes have connectivity and permissions to access the remote source.

Further references:

* [`ais promote` command](#promote-files-and-directories)
* [blog: promoting local and shared files](https://aistore.nvidia.com/blog/2022/03/17/promote)

## Tips

1. **Use `--retries` option**

Including `--retries` in your command will help resolve an occasional timeout and other intermittent failures. For example, `--retries 5` will retry a failed requests up to 5 (five) times.

```console
$ ais put --help
...

   --retries value      when failing to PUT retry the operation up to so many times (with increasing timeout if timed out) (default: 1)
```

2. **Use `--num-workers` option**

In other words, take advantage of the client side multi-threading. If you have sufficient resources, increase this number to allow more workers to transfer data in parallel.

```console
$ ais put --help
...

   --num-workers value  number of concurrent client-side workers (to execute PUT or append requests);
                        use (-1) to indicate single-threaded serial execution (ie., no workers);
                        any positive value will be adjusted _not_ to exceed twice the number of client CPUs (default: 10)
```

### Example 1

Recursively copy the contents of (NFS-mounted) `target_dir/` to the `ais://nnn/target_dir/` bucket, using 64 client workers (OS threads) and retrying failed requests up to 3 times.

```console
$ ais object put -r -y --num-workers 64 --retries 3 target_dir/ ais://nnn/target_dir/
```

### Example 2

Same as above (and notice `ais put` shortcut and `--include-src-dir` option):

```console
$ ais put target_dir ais://nnn -r -y --num-workers 64 --retries 3 --include-src-dir
```

### Example 3

Same as above, but with additional capability to "continue on error" - skip errors that may arise when traversing the source tree:

```console
$ ais put target_dir ais://nnn --recursive --yes --num-workers 64 --retries 3 --include-src-dir --cont-on-err
```

### Example 4

Same as above, but in addition ask CLI to report all errors that may be skipped or ignored due to the `--cont-on-err` flag:

```console
$ ais config cli verbose
PROPERTY         VALUE
verbose          false

$ ais config cli set verbose true
"verbose" set to: "true" (was: "false")

$ ais put target_dir ais://nnn --recursive --yes --num-workers 64 --retries 3 --include-src-dir --cont-on-err
```


3. **Patience**

Be patient: copying from remote locations is subject to network and remote servers' delays, both.

Also and separately, note that at the time of this writing AIS CLI does not support _pagination_ of the remote directories that _may_ contain millions of entries. Listing of the entire remote source is (currently) done in one shot, and prior to copying.

If `ais put` process seems to have paused, there's a good chance it is still listing remote files or copying in the background.

Refrain from pressing `Ctrl-C` to interrupt it.

4. **When your destination bucket is S3 or similar**

Waiting time may be even greater if you are copying data to an AIStore `s3://`, `gs://`, or `az://` bucket. AIS uses write-through, so the same data is written to the remote backend and locally as one atomic transaction.

5. Finally, try to transition to **WebDataset formatting**

Copying, or generally, working in any shape and form with many (millions of) small files comes with significant and unavoidable overhead, both networking and storage-wise.

Use our `ishard` tool to convert and serialize your data using the preferred formatting (a.k.a. WebDataset convention):

* [`ishard` readme](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)
* [`ishard` blog](https://aistore.nvidia.com/blog/2024/08/16/ishard)

# Promote files and directories

Inline help follows below:

```console
$ ais object promote --help
NAME:
   ais object promote - PROMOTE target-accessible files and directories.
   The operation is intended for copying NFS and SMB shares mounted on any/all targets
   but can be also used to copy local files (again, on any/all targets in the cluster).
   Copied files and directories become regular stored objects that can be further listed and operated upon.
   Destination naming is consistent with 'ais put' command, e.g.:
     - 'promote /tmp/subdir/f1 ais://nnn'        - ais://nnn/f1
     - 'promote /tmp/subdir/f2 ais://nnn/aaa'    - ais://nnn/aaa
     - 'promote /tmp/subdir/f3 ais://nnn/aaa/'   - ais://nnn/aaa/f3
     - 'promote /tmp/subdir ais://nnn'           - ais://nnn/f1, ais://nnn/f2, ais://nnn/f3
     - 'promote /tmp/subdir ais://nnn/aaa/'      - ais://nnn/aaa/f1, ais://nnn/aaa/f2, ais://nnn/aaa/f3
   Other supported options follow below.

USAGE:
   ais object promote FILE|DIRECTORY[/PATTERN] BUCKET[/OBJECT_NAME_or_PREFIX] [command options]

OPTIONS:
   --recursive, -r      recursive operation
   --overwrite-dst, -o  overwrite destination, if exists
   --not-file-share     each target must act autonomously skipping file-share auto-detection and promoting the entire source (as seen from the target)
   --delete-src         delete successfully promoted source
   --target-id value    ais target designated to carry out the entire operation
   --verbose, -v        verbose output
   --help, -h           show help
```

## Options

```console
$ ais object promote --help

NAME:
   ais object promote - PROMOTE target-accessible files and directories.
   The operation is intended for copying NFS and SMB shares mounted on any/all targets
   but can be also used to copy local files (again, on any/all targets in the cluster).
   Copied files and directories become regular stored objects that can be further listed and operated upon.
   Destination naming is consistent with 'ais put' command, e.g.:
     - 'promote /tmp/subdir/f1 ais://nnn'        - ais://nnn/f1
     - 'promote /tmp/subdir/f2 ais://nnn/aaa'    - ais://nnn/aaa
     - 'promote /tmp/subdir/f3 ais://nnn/aaa/'   - ais://nnn/aaa/f3
     - 'promote /tmp/subdir ais://nnn'           - ais://nnn/f1, ais://nnn/f2, ais://nnn/f3
     - 'promote /tmp/subdir ais://nnn/aaa/'      - ais://nnn/aaa/f1, ais://nnn/aaa/f2, ais://nnn/aaa/f3
   Other supported options follow below.

USAGE:
   ais object promote FILE|DIRECTORY[/PATTERN] BUCKET[/OBJECT_NAME_or_PREFIX] [command options]

OPTIONS:
   --delete-src         Delete successfully promoted source
   --not-file-share     Each target must act autonomously skipping file-share auto-detection and promoting the entire source (as seen from the target)
   --overwrite-dst, -o  Overwrite destination, if exists
   --recursive, -r      Recursive operation
   --skip-lookup        Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                         1) adding remote bucket to aistore without first checking the bucket's accessibility
                            (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                         2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --target-id value    AIS target designated to carry out the entire operation
   --verbose, -v        Verbose output
   --help, -h           Show help
```

## Destination naming

See above.

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

# Multipart upload

`ais object multipart-upload` or, same, `ais object mpu` - Upload large objects in multiple parts for improved performance and reliability.

Multipart upload allows you to upload large objects by breaking them into smaller, manageable parts. This provides several benefits:

* **Improved performance**: Parts can be uploaded in parallel
* **Reliability**: Failed part uploads can be retried without affecting other parts  
* **Flexibility**: Parts can be uploaded in any order
* **Resume capability**: Ability to abort and restart uploads

The multipart upload process consists of three main steps:
1. **Create** a multipart upload session to get an upload ID
2. **Upload parts** using the upload ID (parts can be uploaded in parallel)
3. **Complete** the upload by assembling all parts into the final object

## Options

```console
$ ais object mpu --help

NAME:
   ais object multipart-upload - Multipart upload operations: create, put parts, complete, and abort

USAGE:
   ais object multipart-upload command [arguments...]  [command options]

COMMANDS:
   create    Create a multipart upload session for large objects.
             Returns an upload ID that must be used for subsequent part uploads and completion, e.g.:
               - 'mpu create ais://bucket/large-file.dat'           - create session for large-file.dat;
               - 'mpu create ais://bucket/video.mp4 --verbose'    - create with verbose output.
   put-part  Upload individual parts for a multipart upload session.
             Parts can be uploaded in parallel and in any order, e.g.:
               - 'mpu put-part ais://bucket/large-file uploadID 1 /path/part1.dat'                            - upload part 1;
               - 'mpu put-part ais://bucket/large-file uploadID 2 /path/part2.dat --verbose'                  - upload part 2 with progress;
               - 'mpu put-part ais://bucket/large-file --upload-id uploadID --part-number 3 /path/part3.dat'  - using flags.
   complete  Complete a multipart upload by assembling all uploaded parts into the final object.
             Parts are assembled in the order specified by part numbers, e.g.:
               - 'mpu complete ais://bucket/large-file uploadID 1,2,3,4,5'                    - assemble 5 parts in order;
               - 'mpu complete ais://bucket/large-file --upload-id uploadID --part-numbers 1,2,3'  - using flags;
               - 'mpu complete ais://bucket/large-file uploadID "1,2,3" --verbose'            - with completion progress.
   abort     Abort a multipart upload session and clean up any uploaded parts.
             All uploaded parts are discarded and the object is not created, e.g.:
               - 'mpu abort ais://bucket/large-file uploadID'                       - abort upload session;
               - 'mpu abort ais://bucket/large-file --upload-id uploadID --verbose' - abort with verbose output.

OPTIONS:
   --help, -h  Show help
```

## Create multipart upload

`ais object mpu create BUCKET/OBJECT_NAME`

Creates a new multipart upload session and returns an upload ID that must be used for all subsequent operations on this upload.

### Create a multipart upload session

```console
$ ais object mpu create ais://mybucket/large-video.mp4
Upload ID: abc123def456

$ ais object mpu create ais://mybucket/large-dataset.tar --verbose
Created multipart upload for ais://mybucket/large-dataset.tar
Upload ID: xyz789uvw012
```

## Upload parts

`ais object mpu put-part BUCKET/OBJECT_NAME UPLOAD_ID PART_NUMBER FILE_PATH`

Uploads individual parts for a multipart upload session. Parts can be uploaded in parallel and in any order.

### Upload parts sequentially

```console
# Upload part 1
$ ais object mpu put-part ais://mybucket/large-video.mp4 abc123def456 1 /tmp/video-part1.mp4 --verbose
Uploading part 1 from /tmp/video-part1.mp4 (524.29MiB)...
Uploaded part 1 for ais://mybucket/large-video.mp4 (upload ID: abc123def456)

# Upload part 2  
$ ais object mpu put-part ais://mybucket/large-video.mp4 abc123def456 2 /tmp/video-part2.mp4 --verbose
Uploading part 2 from /tmp/video-part2.mp4 (524.29MiB)...
Uploaded part 2 for ais://mybucket/large-video.mp4 (upload ID: abc123def456)

# Upload part 3
$ ais object mpu put-part ais://mybucket/large-video.mp4 abc123def456 3 /tmp/video-part3.mp4 --verbose
Uploading part 3 from /tmp/video-part3.mp4 (451.42MiB)...
Uploaded part 3 for ais://mybucket/large-video.mp4 (upload ID: abc123def456)
```

### Upload parts in parallel

Parts can be uploaded simultaneously from different terminals or scripts:

```console
# Terminal 1
$ ais object mpu put-part ais://mybucket/large-file.dat uploadID 1 /tmp/part1.dat

# Terminal 2 (running simultaneously)
$ ais object mpu put-part ais://mybucket/large-file.dat uploadID 2 /tmp/part2.dat

# Terminal 3 (running simultaneously)  
$ ais object mpu put-part ais://mybucket/large-file.dat uploadID 3 /tmp/part3.dat
```

All three commands can be executed at the same time, allowing for faster upload of large files.

## Complete multipart upload

`ais object mpu complete BUCKET/OBJECT_NAME UPLOAD_ID PART_NUMBERS`

Completes a multipart upload by assembling all uploaded parts into the final object. Parts are assembled in the order specified by the part numbers.

### Complete upload with all parts

```console
$ ais object mpu complete ais://mybucket/large-video.mp4 abc123def456 1,2,3 --verbose
Completing multipart upload for ais://mybucket/large-video.mp4 with 3 parts...
Successfully completed multipart upload for ais://mybucket/large-video.mp4

# Verify the object was created
$ ais object ls ais://mybucket --props size
NAME                SIZE
large-video.mp4     1.50GiB
```

### Complete upload using flags

```console
$ ais object mpu complete ais://mybucket/large-dataset.tar --upload-id xyz789uvw012 --part-numbers 1,2,3,4,5
Successfully completed multipart upload for ais://mybucket/large-dataset.tar
```

### Complete upload with verbose progress

```console
$ ais object mpu complete ais://mybucket/large-file.dat uploadID "1,2,3,4,5,6,7,8" --verbose
Completing multipart upload for ais://mybucket/large-file.dat with 8 parts...
Successfully completed multipart upload for ais://mybucket/large-file.dat
```

## Abort multipart upload

`ais object mpu abort BUCKET/OBJECT_NAME UPLOAD_ID`

Aborts a multipart upload session and cleans up any uploaded parts. All uploaded parts are discarded and the object is not created.

### Abort an upload session

```console
$ ais object mpu abort ais://mybucket/large-video.mp4 abc123def456 --verbose
Aborting multipart upload for ais://mybucket/large-video.mp4 (upload ID: abc123def456)...
Successfully aborted multipart upload for ais://mybucket/large-video.mp4

# Verify the object was not created
$ ais object ls ais://mybucket
NAME                SIZE
# (no large-video.mp4 object)
```

## Example: Complete multipart upload workflow

Here's a complete example demonstrating the entire multipart upload process:

```console
# 1. Create bucket
$ ais bucket create ais://mybucket
"ais://mybucket" created

# 2. Split a large file into parts (example using split command)
$ split -b 100M /path/to/large-file.dat /tmp/part-
$ ls /tmp/part-*
/tmp/part-aa /tmp/part-ab /tmp/part-ac /tmp/part-ad

# 3. Create multipart upload session
$ ais object mpu create ais://mybucket/large-file.dat
Upload ID: mpt123xyz789

# 4. Upload all parts
$ ais object mpu put-part ais://mybucket/large-file.dat mpt123xyz789 1 /tmp/part-aa --verbose
$ ais object mpu put-part ais://mybucket/large-file.dat mpt123xyz789 2 /tmp/part-ab --verbose  
$ ais object mpu put-part ais://mybucket/large-file.dat mpt123xyz789 3 /tmp/part-ac --verbose
$ ais object mpu put-part ais://mybucket/large-file.dat mpt123xyz789 4 /tmp/part-ad --verbose

# 5. Complete the upload
$ ais object mpu complete ais://mybucket/large-file.dat mpt123xyz789 1,2,3,4 --verbose
Successfully completed multipart upload for ais://mybucket/large-file.dat

# 6. Verify the final object
$ ais object show ais://mybucket/large-file.dat --props size
PROPERTY    VALUE
size        400.00MiB
```

# Append object

Append operation (not to confuse with appending or [adding to existing archive](/docs/cli/archive.md)) can be executed in 3 different ways:

* using `ais put` with `--append` option;
* using `ais object concat`;
and finally
* writing from standard input with chunk size (ie., `--chunk-size`) small enough to require (appending) multiple chunks.

Here're some examples:

```console
## append all files from a given directory as a single object:

$ ais put docs ais://nnn/all-docs --append

Created ais://nnn/all-docs (size 571.45KiB)
$ ais ls ais://nnn/all-docs -props all
PROPERTY         VALUE
atime            11 Dec 23 12:18 EST
checksum         xxhash[f0eac0698e2489ff]
copies           1 [/ais/mp1/7]
custom           -
ec               -
location         t[VQWtTyuI]:mp[/ais/mp1/7, nvme0n1]
name             ais://nnn/all-docs
size             571.45KiB
version          1
```

```console
## overwrite existing object with 4KiB of random data;
## note that the operation (below) will write about 410 chunks from standard input

$ head -c 4096 /dev/urandom | ais object put - ais://nnn/all-docs --chunk-size 10
PUT (standard input) => ais://nnn/all-docs

$ ais ls ais://nnn/all-docs -props all
PROPERTY         VALUE
atime            11 Dec 23 12:21 EST
checksum         xxhash[b5edf46a1b9459fb]
copies           1 [/ais/mp1/7]
custom           -
ec               -
location         t[VQWtTyuI]:mp[/ais/mp1/7, nvme0n1]
name             ais://nnn/all-docs
size             4.00KiB
version          3
```

# Delete object

`ais object rm` or (same) `ais rmo` - Delete an object or list/range of objects from a bucket.

```console
$ ais rmo --help
NAME:
   ais rmo - (alias for "object rm") Remove object or selected objects from the specified bucket, or buckets - e.g.:
     - 'rm ais://nnn --all'                                   - remove all objects from the bucket ais://nnn;
     - 'rm s3://abc' --all                                    - remove all objects including those that are not _present_ in the cluster;
     - 'rm gs://abc --prefix images/'                         - remove all objects from the virtual subdirectory "images";
     - 'rm gs://abc/images/'                                  - same as above;
     - 'rm gs://abc --template images/'                       - same as above;
     - 'rm gs://abc --template "shard-{0000..9999}.tar.lz4"'  - remove the matching range (prefix + brace expansion);
     - 'rm "gs://abc/shard-{0000..9999}.tar.lz4"'             - same as above (notice double quotes)

USAGE:
   ais rmo BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...] [command options]

OPTIONS:
   --all                  Remove all objects (use with extreme caution!)
   --list value           Comma-separated list of object or file names, e.g.:
                          --list 'o1,o2,o3'
                          --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                          or, when listing files and/or directories:
                          --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --non-recursive, --nr  Non-recursive operation, e.g.:
                          - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                          - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                          - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                          - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --non-verbose, --nv    Non-verbose (quiet) output, minimized reporting, fewer warnings
   --prefix value         Select virtual directories or objects with names starting with the specified prefix, e.g.:
                          '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                          '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   --progress             Show progress bar(s) and progress of execution in real time
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --skip-lookup          Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                           1) adding remote bucket to aistore without first checking the bucket's accessibility
                              (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                           2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --template value       Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                          (with optional steps and gaps), e.g.:
                          --template "" # (an empty or '*' template matches everything)
                          --template 'dir/subdir/'
                          --template 'shard-{1000..9999}.tar'
                          --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                          and similarly, when specifying files and directories:
                          --template '/home/dir/subdir/'
                          --template "/abc/prefix-{0010..9999..2}-suffix"
   --timeout value        Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --verbose, -v          Verbose output
   --wait                 Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --yes, -y              Assume 'yes' to all questions
   --help, -h             Show help
```

> For multi-object delete operation, see also [Operations on Lists and Ranges (and entire buckets)](#operations-on-lists-and-ranges-and-entire-buckets) below.

## Disambiguating multi-object operation

Let's say, in its initial state the bucket consists of:

```console
$ ais ls s3://mybucket
NAME                                     SIZE            CACHED
README.md                                16.26KiB        no
aaa                                      16.26KiB        yes
aaa/bbb/111                              16.26KiB        no
aaa/bbb/ccc/README.md                    5.09KiB         no
...
```

**Notice** that `aaa` here is both an object and a virtual directory.

That's why:

```console
$ ais rmo s3://mybucket/aaa
Error: part of the URI "aaa" can be interpreted as an object name and/or mutli-object matching prefix
(Tip:  to disambiguate, use either '--non-recursive' or '--prefix')
```

And so, as per the Tip (above), we can go ahead and disambiguate one way or another, e.g.:

```console
$ ais rmo s3://mybucket/aaa --nr
deleted "aaa" from s3://mybucket

$ ais ls s3://mybucket
NAME                                     SIZE            CACHED
README.md                                16.26KiB        no
aaa/bbb/111                              16.26KiB        no
aaa/bbb/ccc/README.md                    5.09KiB         no
...
```

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
* For multi-object delete that operates on a `--list` or `--template`, please see: [Operations on Lists and Ranges (and entire buckets)](#operations-on-lists-and-ranges-and-entire-buckets) below.

# Evict one remote bucket, multiple remote buckets, or selected objects in a given remote bucket or buckets

Some of the supported functionality can be quickly demonstrated with the following examples:

* [CLI: Three Ways to Evict Remote Bucket](/docs/cli/evicting_buckets_andor_data.md)


```console
$ ais evict --help
NAME:
   ais evict - (alias for "bucket evict") Evict one remote bucket, multiple remote buckets, or
     selected objects in a given remote bucket or buckets,
     e.g.:
     - evict gs://abc                                          - evict entire bucket from aistore: remove all "cached" gs://abc objects _and_ bucket metadata;
     - evict gs://abc --keep-md                                - same as above but keep bucket metadata;
     - evict gs:                                               - evict all GCP buckets from the cluster;
     - evict gs://abc --prefix images/                         - evict all gs://abc objects from the virtual subdirectory "images";
     - evict gs://abc/images/                                  - same as above;
     - evict gs://abc/images/ --nr                             - same as above, but do not recurse into virtual subdirs;
     - evict gs://abc --template images/                       - same as above;
     - evict gs://abc --template "shard-{0000..9999}.tar.lz4"  - evict the matching range (prefix + brace expansion);
     - evict "gs://abc/shard-{0000..9999}.tar.lz4"             - same as above (notice BUCKET/TEMPLATE argument in quotes)

USAGE:
   ais evict BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...] [command options]

OPTIONS:
   dry-run           Preview the results without really running the action
   keep-md,k         Keep bucket metadata
   list              Comma-separated list of object or file names, e.g.:
                     --list 'o1,o2,o3'
                     --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                     or, when listing files and/or directories:
                     --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   non-recursive,nr  Non-recursive operation, e.g.:
                     - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                     - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                     - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                     - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   non-verbose,nv    Non-verbose (quiet) output, minimized reporting, fewer warnings
   prefix            Select virtual directories or objects with names starting with the specified prefix, e.g.:
                     '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                     '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   progress          Show progress bar(s) and progress of execution in real time
   refresh           Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                     valid time units: ns, us (or µs), ms, s (default), m, h
   skip-lookup       Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                      1) adding remote bucket to aistore without first checking the bucket's accessibility
                         (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                      2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   template          Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                     (with optional steps and gaps), e.g.:
                     --template "" # (an empty or '*' template matches everything)
                     --template 'dir/subdir/'
                     --template 'shard-{1000..9999}.tar'
                     --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                     and similarly, when specifying files and directories:
                     --template '/home/dir/subdir/'
                     --template "/abc/prefix-{0010..9999..2}-suffix"
   timeout           Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   verbose,v         Verbose output
   wait              Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   help, h           Show help
```

[Evict](/docs/bucket.md#prefetchevict-objects) object(s) from a bucket that has [remote backend](/docs/bucket.md).

* NOTE: for each space-separated object name CLI sends a separate request.
* For multi-object eviction that operates on a `--list` or `--template`, please see: [Operations on Lists and Ranges (and entire buckets)](#operations-on-lists-and-ranges-and-entire-buckets) below.
* Similar to delete, prefetch and copy operations, `evict` also supports embedded prefix - see [disambiguating multi-object operation](#disambiguating-multi-object-operation)

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

```console
$ ais object concat --help

NAME:
   ais object concat - Append a file, a directory, or multiple files and/or directories
   as a new BUCKET/OBJECT_NAME if doesn't exists, and to an existing BUCKET/OBJECT_NAME otherwise, e.g.:
   $ ais object concat docs ais://nnn/all-docs ### concatenate all files from docs/ directory.

USAGE:
   ais object concat FILE|DIRECTORY[/PATTERN] [ FILE|DIRECTORY[/PATTERN] ...] BUCKET/OBJECT_NAME [command options]

OPTIONS:
   --progress       Show progress bar(s) and progress of execution in real time
   --recursive, -r  Recursive operation
   --units value    Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                    iec - IEC format, e.g.: KiB, MiB, GiB (default)
                    si  - SI (metric) format, e.g.: KB, MB, GB
                    raw - do not convert to (or from) human-readable format
   --help, -h       Show help
```

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

`ais object set-custom BUCKET/OBJECT_NAME JSON_SPECIFICATION|KEY=VALUE [KEY=VALUE...]`, [command options]

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

# Operations on Lists and Ranges (and entire buckets)

Generally, multi-object operations are supported in 2 different ways:

1. specifying source directory in the command line - see e.g. [Promote files and directories](#promote-files-and-directories) and [Concat objects](#concat-objects);
2. via `--list` or `--template` options, whereby the latter supports Bash expansion syntax and can also contain prefix, such as a virtual parent directory, etc.)

This section documents and exemplifies AIS CLI operating on multiple (source) objects that you can specify either explicitly or implicitly
using the `--list` or `--template` flags.

The number of objects "involved" in a single operation does not have any designed-in limitations: all AIS targets work on a given multi-object operation simultaneously and in parallel.

* **See also:** [List/Range Operations](/docs/batch.md#listrange-operations).

## Prefetch objects

This is `ais start prefetch` or, same, `ais prefetch` command:

```console
$ ais prefetch --help
NAME:
   ais prefetch - (alias for "object prefetch") Prefetch one remote bucket, multiple remote buckets, or
   selected objects in a given remote bucket or buckets, e.g.:
     - 'prefetch gs://abc'                                          - prefetch entire bucket (all gs://abc objects that are _not_ in-cluster);
     - 'prefetch gs://abc --num-workers 32'                         - same as above with 32 concurrent (prefetching) workers;
     - 'prefetch gs:'                                               - prefetch all visible/accessible GCP buckets;
     - 'prefetch gs: --num-workers=48'                              - same as above employing 48 workers;
     - 'prefetch gs://abc --prefix images/'                         - prefetch all objects from the virtual subdirectory "images";
     - 'prefetch gs://abc --prefix images/ --nr'                    - prefetch only immediate contents of "images/" (non-recursive);
     - 'prefetch gs://abc --template images/'                       - same as above;
     - 'prefetch gs://abc/images/'                                  - same as above;
     - 'prefetch gs://abc --template "shard-{0000..9999}.tar.lz4"'  - prefetch the matching range (prefix + brace expansion);
     - 'prefetch "gs://abc/shard-{0000..9999}.tar.lz4"'             - same as above (notice double quotes)

USAGE:
   ais prefetch BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...] [command options]

OPTIONS:
   --blob-threshold value  Utilize built-in blob-downloader for remote objects greater than the specified (threshold) size
                           in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   --dry-run               Preview the results without really running the action
   --latest                Check in-cluster metadata and, possibly, GET, download, prefetch, or otherwise copy the latest object version
                           from the associated remote bucket;
                           the option provides operation-level control over object versioning (and version synchronization)
                           without the need to change the corresponding bucket configuration: 'versioning.validate_warm_get';
                           see also:
                             - 'ais show bucket BUCKET versioning'
                             - 'ais bucket props set BUCKET versioning'
                             - 'ais ls --check-versions'
                           supported commands include:
                             - 'ais cp', 'ais prefetch', 'ais get'
   --list value            Comma-separated list of object or file names, e.g.:
                           --list 'o1,o2,o3'
                           --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                           or, when listing files and/or directories:
                           --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --non-recursive, --nr   Non-recursive operation, e.g.:
                           - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                           - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                           - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                           - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --num-workers value     Number of concurrent workers (readers); defaults to a number of target mountpaths if omitted or zero;
                           use (-1) to indicate single-threaded serial execution (ie., no workers);
                           any positive value will be adjusted _not_ to exceed the number of target CPUs (default: 0)
   --prefix value          Select virtual directories or objects with names starting with the specified prefix, e.g.:
                           '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                           '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   --progress              Show progress bar(s) and progress of execution in real time
   --refresh value         Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                           valid time units: ns, us (or µs), ms, s (default), m, h
   --skip-lookup           Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                            1) adding remote bucket to aistore without first checking the bucket's accessibility
                               (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                            2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --template value        Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                           (with optional steps and gaps), e.g.:
                           --template "" # (an empty or '*' template matches everything)
                           --template 'dir/subdir/'
                           --template 'shard-{1000..9999}.tar'
                           --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                           and similarly, when specifying files and directories:
                           --template '/home/dir/subdir/'
                           --template "/abc/prefix-{0010..9999..2}-suffix"
   --timeout value         Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                           valid time units: ns, us (or µs), ms, s (default), m, h
   --wait                  Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --yes, -y               Assume 'yes' to all questions
   --help, -h              Show help
```

Note usage examples above. You can always run `--help` option to see the most recently updated inline help.

### See also
* [Prefetch/Evict objects](/docs/bucket.md#prefetchevict-objects)
* Similar to delete, evict and copy operations, `prefetch`also supports embedded prefix - see [disambiguating multi-object operation](#disambiguating-multi-object-operation)

## Example prefetching objects

This example demonstrates how to prefetch objects from a remote bucket, and how to monitor the progress of the operation.

### Checking cached objects

First, let's check which objects are currently stored in-cluster (if any):

```console
$ ais ls s3://cloud-bucket --cached
NAME     SIZE
1000052  1.00MiB
10000a2  1.00MiB
10000b4  1.00MiB
10000bd  1.00MiB
...
```

### Evicting cached objects

To remove all in-cluster content while preserving the bucket's metadata:

> The terms **in-cluster** and **cached** are used interchangeably throughout the entire documentation and CLI.

```console
$ ais evict s3://cloud-bucket --keep-md
Evicted s3://cloud-bucket contents from aistore: the bucket is now empty

$ ais ls s3://cloud-bucket --cached
NAME     SIZE
```

### Prefetching objects

To prefetch objects with a specific prefix from a cloud bucket:

```console
$ ais prefetch s3://cloud-bucket --prefix 10 --num-workers 16

prefetch-objects[MV4ex8u6h]: prefetch "10" from s3://cloud-bucket. To monitor the progress, run 'ais show job MV4ex8u6h'
```

> The prefix in the example is "10"

### Monitoring progress

You can monitor the progress of the prefetch operation using the `ais show job prefetch` command. Add the `--refresh` flag followed by a time in seconds to get automatic updates:

```console
$ ais show job prefetch

prefetch-objects[MV4ex8u6h] (run options: prefix:10, workers: 16, parallelism: w[16] chan-full[0,6])
NODE             ID              KIND                    BUCKET          OBJECTS         BYTES           START           END     STATE
KactABCD         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 5               5.00MiB         18:28:55        -       Running
XXytEFGH         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 4               4.00MiB         18:28:55        -       Running
YMjtIJKL         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 5               5.00MiB         18:28:55        -       Running
oJXtMNOP         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 6               6.00MiB         18:28:55        -       Running
vWrtQRST         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 5               5.00MiB         18:28:55        -       Running
ybTtUVWX         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 5               5.00MiB         18:28:55        -       Running
                                Total:                                  30              30.00MiB ✓
```

The output shows statistics for each node in the AIStore cluster:
- NODE: The name of the node
- ID: The job ID
- KIND: The type of operation
- BUCKET: Source bucket
- OBJECTS: Number of objects processed
- BYTES: Amount of data prefetched
- START: Job start time
- END: Job end time (empty if job is still running)
- STATE: Current job state

The output also includes a "Total" row at the bottom that provides cluster-wide aggregated values for the number of objects prefetched and bytes transferred. The checkmark (✓) indicates that all nodes are reporting byte statistics.

You can see the progress over time with automatic refresh:

```console
$ ais show job prefetch --refresh 10

prefetch-objects[MV4ex8u6h] (run options: prefix:10, workers: 16, parallelism: w[16] chan-full[8,32])
NODE             ID              KIND                    BUCKET          OBJECTS         BYTES           START           END     STATE
KactABCD         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 27              27.00MiB        18:28:55        -       Running
XXytEFGH         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 23              23.00MiB        18:28:55        -       Running
YMjtIJKL         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 41              41.00MiB        18:28:55        -       Running
oJXtMNOP         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 34              34.00MiB        18:28:55        -       Running
vWrtQRST         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 23              23.00MiB        18:28:55        -       Running
ybTtUVWX         MV4ex8u6h       prefetch-listrange      s3://cloud-bucket 31              31.00MiB        18:28:55        -       Running
                                Total:                                  179             179.00MiB ✓
```

### Stopping jobs

To stop all in-progress jobs:

```console
$ ais stop --all
```

This will stop all running jobs. To stop a specific job, use `ais stop job JOB_ID`.

### Example: prefetch using prefix

Initially:

```console
$ ais ls s3://abc --all --limit 10
NAME     SIZE            CACHED  STATUS
10000a2  10.00MiB        no      n/a
10000b4  10.00MiB        no      n/a
10000bd  10.00MiB        no      n/a
10000d6  10.00MiB        no      n/a
10000ea  10.00MiB        no      n/a
10001a2  10.00MiB        no      n/a
10001b4  10.00MiB        no      n/a
10001bd  10.00MiB        no      n/a
10001d6  10.00MiB        no      n/a
10001ea  10.00MiB        no      n/a
```

Now, let's use `--prefix` option to - in this case - fetch a single object:

```console
$ ais prefetch s3://abc --prefix 10000a2
prefetch-objects[E0e5mq9Kav]: prefetch "10000a2" from s3://abc. To monitor the progress, run 'ais show job E0e5mq9Kav'

$ ais ls s3://abc --all --limit 10
NAME     SIZE            CACHED  STATUS
10000a2  10.00MiB        yes     ok     ### <<<<< in cluster
10000b4  10.00MiB        no      n/a
10000bd  10.00MiB        no      n/a
10000d6  10.00MiB        no      n/a
10000ea  10.00MiB        no      n/a
10001a2  10.00MiB        no      n/a
10001b4  10.00MiB        no      n/a
10001bd  10.00MiB        no      n/a
10001d6  10.00MiB        no      n/a
10001ea  10.00MiB        no      n/a
```

### Example: prefetch using template

Since `--template` can optionally contain prefix and zero or more _ranges_, we could execute the above example as follows:

```console
$ ais prefetch s3://abc --template 10000a2
```

This, in fact, would produce the same result (see previous section).

But of course, "templated" match can also specify an actual range, for example:

```console
$ ais ls gs://nnn --all --limit 5
NAME     SIZE            CACHED  STATUS
shard-001  1.00MiB       no      n/a
shard-002  1.00MiB       no      n/a
shard-003  1.00MiB       no      n/a
shard-004  1.00MiB       no      n/a
shard-005  1.00MiB       no      n/a

$ ais prefetch gs://nnn --template --template "shard-{001..003}"

$ ais ls gs://nnn --all --limit 5
NAME     SIZE            CACHED  STATUS
shard-001  1.00MiB       yes     ok
shard-002  1.00MiB       yes     ok
shard-003  1.00MiB       yes     ok
shard-004  1.00MiB       no      n/a
shard-005  1.00MiB       no      n/a
```

### Example: prefetch a list of objects

NOTE: make sure to use double or single quotations to specify the list, as shown below.

```console
# Prefetch o1, o2, and o3 from AWS bucket `cloudbucket`:
$ ais prefetch aws://cloudbucket --list 'o1,o2,o3'
```

### Example: prefetch a range of objects

```console
# Prefetch from AWS bucket `cloudbucket` all objects in the specified range.
# NOTE: make sure to use double or single quotations to specify the template (aka "range")

$ ais prefetch aws://cloudbucket --template "shard-{001..999}.tar"
```

## Delete multiple objects

`ais object rm BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...] [command options]`

Delete an object or list or range of objects from a bucket.

Alias: `ais rmo`.

### Options

```console
$ ais object rm --help
NAME:
   ais object rm - Remove object or selected objects from the specified bucket, or buckets - e.g.:
     - 'rm ais://nnn --all'                                   - remove all objects from the bucket ais://nnn;
     - 'rm s3://abc' --all                                    - remove all objects including those that are not _present_ in the cluster;
     - 'rm gs://abc --prefix images/'                         - remove all objects from the virtual subdirectory "images";
     - 'rm gs://abc/images/'                                  - same as above;
     - 'rm gs://abc/images/ --nr'                             - same as above, but do not recurse into virtual subdirs;
     - 'rm gs://abc --template images/'                       - same as above;
     - 'rm gs://abc --template "shard-{0000..9999}.tar.lz4"'  - remove the matching range (prefix + brace expansion);
     - 'rm "gs://abc/shard-{0000..9999}.tar.lz4"'             - same as above (notice BUCKET/TEMPLATE argument in quotes)

USAGE:
   ais object rm BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...] [command options]

OPTIONS:
   --all                  Remove all objects (use with extreme caution!)
   --list value           Comma-separated list of object or file names, e.g.:
                          --list 'o1,o2,o3'
                          --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                          or, when listing files and/or directories:
                          --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --non-recursive, --nr  Non-recursive operation, e.g.:
                          - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                          - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                          - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                          - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --non-verbose, --nv    Non-verbose (quiet) output, minimized reporting, fewer warnings
   --prefix value         Select virtual directories or objects with names starting with the specified prefix, e.g.:
                          '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                          '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   --progress             Show progress bar(s) and progress of execution in real time
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --skip-lookup          Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                           1) adding remote bucket to aistore without first checking the bucket's accessibility
                              (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                           2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --template value       Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                          (with optional steps and gaps), e.g.:
                          --template "" # (an empty or '*' template matches everything)
                          --template 'dir/subdir/'
                          --template 'shard-{1000..9999}.tar'
                          --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                          and similarly, when specifying files and directories:
                          --template '/home/dir/subdir/'
                          --template "/abc/prefix-{0010..9999..2}-suffix"
   --timeout value        Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --verbose, -v          Verbose output
   --wait                 Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --yes, -y              Assume 'yes' to all questions
   --help, -h             Show help
```

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

### See also:

To fully synchronize in-cluster content with remote backend, please refer to [out of band updates](/docs/out_of_band.md)

## Evict multiple objects

`ais evict BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...]` [command options]

Command `ais evict` is a shorter version of `ais bucket evict`.

### Options

Here's inline help, and specifically notice the _multi-object_ options: `--template`, `--list`, and `--prefix`:

```concole
$ ais evict --help
NAME:
   ais evict - (alias for "bucket evict") Evict one remote bucket, multiple remote buckets, or
     selected objects in a given remote bucket or buckets,
     e.g.:
     - evict gs://abc                                          - evict entire bucket from aistore: remove all "cached" gs://abc objects _and_ bucket metadata;
     - evict gs://abc --keep-md                                - same as above but keep bucket metadata;
     - evict gs:                                               - evict all GCP buckets from the cluster;
     - evict gs://abc --prefix images/                         - evict all gs://abc objects from the virtual subdirectory "images";
     - evict gs://abc/images/                                  - same as above;
     - evict gs://abc/images/ --nr                             - same as above, but do not recurse into virtual subdirs;
     - evict gs://abc --template images/                       - same as above;
     - evict gs://abc --template "shard-{0000..9999}.tar.lz4"  - evict the matching range (prefix + brace expansion);
     - evict "gs://abc/shard-{0000..9999}.tar.lz4"             - same as above (notice BUCKET/TEMPLATE argument in quotes)

USAGE:
   ais evict BUCKET[/OBJECT_NAME_or_TEMPLATE] [BUCKET[/OBJECT_NAME_or_TEMPLATE] ...] [command options]

OPTIONS:
   dry-run           Preview the results without really running the action
   keep-md           Keep bucket metadata
   list              Comma-separated list of object or file names, e.g.:
                     --list 'o1,o2,o3'
                     --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                     or, when listing files and/or directories:
                     --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   non-recursive,nr  Non-recursive operation, e.g.:
                     - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                     - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                     - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                     - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   non-verbose,nv    Non-verbose (quiet) output, minimized reporting, fewer warnings
   prefix            Select virtual directories or objects with names starting with the specified prefix, e.g.:
                     '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                     '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   progress          Show progress bar(s) and progress of execution in real time
   refresh           Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                     valid time units: ns, us (or µs), ms, s (default), m, h
   skip-lookup       Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                      1) adding remote bucket to aistore without first checking the bucket's accessibility
                         (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                      2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   template          Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                     (with optional steps and gaps), e.g.:
                     --template "" # (an empty or '*' template matches everything)
                     --template 'dir/subdir/'
                     --template 'shard-{1000..9999}.tar'
                     --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                     and similarly, when specifying files and directories:
                     --template '/home/dir/subdir/'
                     --template "/abc/prefix-{0010..9999..2}-suffix"
   timeout           Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   verbose,v         Verbose output
   wait              Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   help, h           Show help
```

Note usage examples above. You can always run `--help` option to see the most recently updated inline help.

### Evict a range of objects

```console
$ ais bucket evict aws://cloudbucket --template "shard-{900..999}.tar"
```
