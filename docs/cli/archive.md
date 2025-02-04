---
layout: post
title: BUCKET
permalink: /docs/cli/archive
redirect_from:
 - /cli/archive.md/
 - /docs/cli/archive.md/
---

# When objects are called _shards_

In this document:
* commands to read, write, extract, and list *archives* - objects formatted as `TAR`, `TGZ` (or `TAR.GZ`) , `ZIP`, or `TAR.LZ4`.

For the most recently updated list of supported archival formats, please refer to [this source](https://github.com/NVIDIA/aistore/blob/main/cmn/archive/mime.go).

The corresponding subset of CLI commands starts with `ais archive`, from where you can `<TAB-TAB>` to the actual (reading, writing, etc.) operation.

```console
$ ais archive get --help

NAME:
   ais archive get - Get a shard and extract its content; get an archived file;
              write the content locally with destination options including: filename, directory, STDOUT ('-'), or '/dev/null' (discard);
              assorted options further include:
              - '--prefix' to get multiple shards in one shot (empty prefix for the entire bucket);
              - '--progress' and '--refresh' to watch progress bar;
              - '-v' to produce verbose output when getting multiple objects.
   'ais archive get' examples:
              - ais://abc/trunk-0123.tar.lz4 /tmp/out - get and extract entire shard to /tmp/out/trunk/*
              - ais://abc/trunk-0123.tar.lz4 --archpath file45.jpeg /tmp/out - extract one named file
              - ais://abc/trunk-0123.tar.lz4/file45.jpeg /tmp/out - same as above (and note that '--archpath' is implied)
              - ais://abc/trunk-0123.tar.lz4/file45 /tmp/out/file456.new - same as above, with destination explicitly (re)named
   'ais archive get' multi-selection examples:
              - ais://abc/trunk-0123.tar 111.tar --archregx=jpeg --archmode=suffix - return 111.tar with all *.jpeg files from a given shard
              - ais://abc/trunk-0123.tar 222.tar --archregx=file45 --archmode=wdskey - return 222.tar with all file45.* files --/--
              - ais://abc/trunk-0123.tar 333.tar --archregx=subdir/ --archmode=prefix - 333.tar with all subdir/* files --/--

USAGE:
   ais archive get BUCKET[/SHARD_NAME] [OUT_FILE|OUT_DIR|-] [command options]

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
   --checksum           Validate checksum
   --chunk-size value   Chunk size in IEC or SI units, or "raw" bytes (e.g.: 4mb, 1MiB, 1048576, 128k; see '--units')
   --extract, -x        Extract all files from archive(s)
   --inv-id value       Bucket inventory ID (optional; by default, we use bucket name as the bucket's inventory ID)
   --inv-name value     Bucket inventory name (optional; system default name is '.inventory')
   --inventory          List objects using _bucket inventory_ (docs/s3inventory.md); requires s3:// backend; will provide significant performance
                        boost when used with very large s3 buckets; e.g. usage:
                          1) 'ais ls s3://abc --inventory'
                          2) 'ais ls s3://abc --inventory --paged --prefix=subdir/'
                        (see also: docs/s3inventory.md)
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
   --limit value        The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                        e.g.:
                        - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                        - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                        - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/-- (default: 0)
   --num-workers value  Number of concurrent blob-downloading workers (readers); system default when omitted or zero (default: 0)
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

## Table of Contents
- [Archive files and directories](#archive-files-and-directories)
- [Append files and directories to an existing archive](#append-files-and-directories-to-an-existing-archive)
- [Archive multiple objects](#archive-multiple-objects)
- [List archived content](#list-archived-content)
- [Get archived content](#get-archived-content)
- [Get archived content: multiple-selection](#get-archived-content-multiple-selection)
- [Generate shards](#generate-shards)

## Archive files and directories

Archive multiple files.

```console
$ ais archive put --help

NAME:
   ais archive put - Archive a file, a directory, or multiple files and/or directories as
     (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object - aka "shard".
     Both APPEND (to an existing shard) and PUT (a new version of the shard) are supported.
     Examples:
     - 'local-file s3://q/shard-00123.tar.lz4 --append --archpath name-in-archive' - append file to a given shard,
        optionally, rename it (inside archive) as specified;
     - 'local-file s3://q/shard-00123.tar.lz4 --append-or-put --archpath name-in-archive' - append file to a given shard if exists,
        otherwise, create a new shard (and name it shard-00123.tar.lz4, as specified);
     - 'src-dir gs://w/shard-999.zip --append' - archive entire 'src-dir' directory; iff the destination .zip doesn't exist create a new one;
     - '"sys, docs" ais://dst/CCC.tar --dry-run -y -r --archpath ggg/' - dry-run to recursively archive two directories.
     Tips:
     - use '--dry-run' if in doubt;
     - to archive objects from a ais:// or remote bucket, run 'ais archive bucket' (see --help for details).

USAGE:
   ais archive put [-|FILE|DIRECTORY[/PATTERN]] BUCKET/SHARD_NAME [command options]

OPTIONS:
   --append             Add newly archived content to the destination object ("archive", "shard") that must exist
   --append-or-put      Append to an existing destination object ("archive", "shard") iff exists; otherwise PUT a new archive (shard);
                        note that PUT (with subsequent overwrite if the destination exists) is the default behavior when the flag is omitted
   --archpath value     Filename in an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4
   --cont-on-err        Keep running archiving xaction (job) in presence of errors in a any given multi-object transaction
   --dry-run            Preview the results without really running the action
   --include-src-dir    Prefix the names of archived files with the (root) source directory
   --list value         Comma-separated list of object or file names, e.g.:
                        --list 'o1,o2,o3'
                        --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                        or, when listing files and/or directories:
                        --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --num-workers value  Number of concurrent client-side workers (to execute PUT or append requests);
                        use (-1) to indicate single-threaded serial execution (ie., no workers);
                        any positive value will be adjusted _not_ to exceed twice the number of client CPUs (default: 10)
   --progress           Show progress bar(s) and progress of execution in real time
   --recursive, -r      Recursive operation
   --refresh value      Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --skip-vc            Skip loading object metadata (and the associated checksum & version related processing)
   --template value     Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                        (with optional steps and gaps), e.g.:
                        --template "" # (an empty or '*' template matches eveything)
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
                        raw - do not convert to (or from) human-readable format
   --verbose, -v        Verbose output
   --wait               Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --yes, -y            Assume 'yes' to all questions
   --help, -h           Show help
```

The operation accepts either an explicitly defined *list* or template-defined *range* of file names (to archive).

**NOTE:**

* `ais archive put` works with locally accessible (source) files and shall _not_ be confused with `ais archive bucket` command (below).

Also, note that `ais put` command with its `--archpath` option provides an alternative way to archive multiple objects:

For the most recently updated list of supported archival formats, please see:

* [this source](https://github.com/NVIDIA/aistore/blob/main/cmn/archive/mime.go).

## Append files and directories to an existing archive

APPEND operation provides for appending files to existing archives (shards). As such, APPEND is a variation of PUT (above) with additional **two boolean flags**:

| Name | Description |
| --- | --- |
| `--append` | add newly archived content to the destination object (\"archive\", \"shard\") that **must** exist |
| `--append-or-put` | **if** destination object (\"archive\", \"shard\") exists append to it, otherwise archive a new one |

### Example 1: add file to archive

#### step 1. create archive (by archiving a given source dir)

```console
$ ais archive put sys ais://nnn/sys.tar.lz4
Warning: multi-file 'archive put' operation requires either '--append' or '--append-or-put' option
Proceed to execute 'archive put --append-or-put'? [Y/N]: y
Files to upload:
EXTENSION        COUNT   SIZE
.go              11      17.46KiB
TOTAL            11      17.46KiB
APPEND 11 files (one directory, non-recursive) => ais://nnn/sys.tar.lz4? [Y/N]: y
Done
```

#### step 2. add a single file to existing archive

```console
$ ais archive put README.md ais://nnn/sys.tar.lz4 --archpath=docs/README --append
APPEND README.md to ais://nnn/sys.tar.lz4 as "docs/README"
```

#### step 3. list entire bucket with an `--archive` option to show all archived entries

```console
$ ais ls ais://nnn --archive
NAME                             SIZE
sys.tar.lz4                      16.84KiB
    sys.tar.lz4/api_linux.go     1.07KiB
    sys.tar.lz4/cpu.go           1.07KiB
    sys.tar.lz4/cpu_darwin.go    802B
    sys.tar.lz4/cpu_linux.go     2.14KiB
    sys.tar.lz4/docs/README      13.85KiB
    sys.tar.lz4/mem.go           1.16KiB
    sys.tar.lz4/mem_darwin.go    2.04KiB
    sys.tar.lz4/mem_linux.go     2.81KiB
    sys.tar.lz4/proc.go          784B
    sys.tar.lz4/proc_darwin.go   369B
    sys.tar.lz4/proc_linux.go    1.40KiB
    sys.tar.lz4/sys_test.go      3.88KiB
Listed: 13 names
```

Alternatively, use regex to select:

```console
$ ais ls ais://nnn --archive --regex docs
NAME                             SIZE
    sys.tar.lz4/docs/README      13.85KiB
```

### Example 2: use `--template` flag to add source files

Generally, the `--template` option combines (an optional) prefix and/or one or more ranges (e.g., bash brace expansions).

In this case, the template we use is a simple prefix with no ranges.

```console
$ ls -l /tmp/w
total 32
-rw-r--r-- 1 root root 14180 Dec 11 18:18 111
-rw-r--r-- 1 root root 14180 Dec 11 18:18 222

$ ais archive put ais://nnn/shard-001.tar --template /tmp/w/ --append
Files to upload:
EXTENSION        COUNT   SIZE
                 2       27.70KiB
TOTAL            2       27.70KiB
APPEND 2 files (one directory, non-recursive) => ais://nnn/shard-001.tar? [Y/N]: y
Done
$ ais ls ais://nnn/shard-001.tar --archive
NAME                                             SIZE
shard-001.tar                                    37.50KiB
    shard-001.tar/111                            13.85KiB
    shard-001.tar/222                            13.85KiB
    shard-001.tar/23ed44d8bf3952a35484-1.test    1.00KiB
    shard-001.tar/452938788ebb87807043-4.test    1.00KiB
    shard-001.tar/7925bc9b5eb1daa12ed0-2.test    1.00KiB
    shard-001.tar/8264574b49bd188a4b27-0.test    1.00KiB
    shard-001.tar/f1f25e52c5edd768e0ec-3.test    1.00KiB
```

### Example 3: add file to archive

In this example, we assume that `arch.tar` already exists.

```console
# contents _before_:
$ ais archive ls ais://abc/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB

# add file to existing archive:
$ ais archive put /tmp/obj1.bin ais://abc/arch.tar --archpath bin/obj1
APPEND "/tmp/obj1.bin" to object "ais://abc/arch.tar[/bin/obj1]"

# contents _after_:
$ ais archive ls ais://abc/arch.tar
NAME                    SIZE
arch.tar                6KiB
    arch.tar/bin/obj1   2.KiB
    arch.tar/obj1       1.0KiB
    arch.tar/obj2       1.0KiB
```

### Example 4: add file to archive

```console
# contents _before_:

$ ais archive ls ais://nnn/shard-2.tar
NAME                                             SIZE
shard-2.tar                                      5.50KiB
    shard-2.tar/0379f37cbb0415e7eaea-3.test      1.00KiB
    shard-2.tar/504c563d14852368575b-5.test      1.00KiB
    shard-2.tar/c7bcb7014568b5e7d13b-4.test      1.00KiB

# append and note that `--archpath` can specify a fully qualified destination name

$ ais archive put LICENSE ais://nnn/shard-2.tar --archpath shard-2.tar/license.test
APPEND "/go/src/github.com/NVIDIA/aistore/LICENSE" to "ais://nnn/shard-2.tar[/shard-2.tar/license.test]"

# contents _after_:
$ ais archive ls ais://nnn/shard-2.tar
NAME                                             SIZE
shard-2.tar                                      7.50KiB
    shard-2.tar/0379f37cbb0415e7eaea-3.test      1.00KiB
    shard-2.tar/504c563d14852368575b-5.test      1.00KiB
    shard-2.tar/c7bcb7014568b5e7d13b-4.test      1.00KiB
    shard-2.tar/license.test                     1.05KiB
```

## Archive multiple objects

This is a yet another archive-**creating** operation that:

1. takes in multiple objects from a given **source bucket**, and
2. archives them all as a shard in the specified destination bucket,

   where:

* source and destination buckets may not necessarily be different;
* both `--list` and `--template` options are supported
* supported archival formats include `.tar`, `.tar.gz` (or, same, `.tgz`), and `.zip`; more extensions may be added in the future.
* archiving is carried out asynchronously, in parallel by all AIS targets.

As such, `ais archive bucket` is one of the supported [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges-and-entire-buckets).

**NOTE:**

* `ais archive bucket` multi-object bucket-to-bucket archiving shall _not_ be confused with `ais archive put` command - the latter is used to archive multiple source **files** from a local (or locally accessible) source **directory**.

```console
$ ais archive bucket --help
NAME:
   ais archive bucket - archive multiple objects from SRC_BUCKET as (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted shard

USAGE:
   ais archive bucket SRC_BUCKET DST_BUCKET/SHARD_NAME [command options]

OPTIONS:
   --template value   template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                      (with optional steps and gaps), e.g.:
                      --template "" # (an empty or '*' template matches eveything)
                      --template 'dir/subdir/'
                      --template 'shard-{1000..9999}.tar'
                      --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                      and similarly, when specifying files and directories:
                      --template '/home/dir/subdir/'
                      --template "/abc/prefix-{0010..9999..2}-suffix"
   --list value       comma-separated list of object or file names, e.g.:
                      --list 'o1,o2,o3'
                      --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                      or, when listing files and/or directories:
                      --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --dry-run          preview the results without really running the action
   --include-src-bck  prefix the names of archived files with the source bucket name
   --append-or-put    if destination object ("archive", "shard") exists append to it, otherwise archive a new one
   --cont-on-err      keep running archiving xaction in presence of errors in a any given multi-object transaction
   --wait             wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --help, -h         show help
```

### Examples

1. Archive a list of objects from a given bucket:

```console
$ ais archive bucket ais://bck/arch.tar --list obj1,obj2
Archiving "ais://bck/arch.tar" ...
```

Resulting `ais://bck/arch.tar` contains objects `ais://bck/obj1` and `ais://bck/obj2`.

2. Archive objects from a different bucket, use template (range):

```console
$ ais archive bucket ais://src ais://dst/arch.tar --template "obj-{0..9}"

Archiving "ais://dst/arch.tar" ...
```

`ais://dst/arch.tar` now contains 10 objects from bucket `ais://src`: `ais://src/obj-0`, `ais://src/obj-1` ... `ais://src/obj-9`.

3. Archive 3 objects and then append 2 more:

```console
$ ais archive bucket ais://bck/arch1.tar --template "obj{1..3}"
Archived "ais://bck/arch1.tar" ...
$ ais archive ls ais://bck/arch1.tar
NAME                     SIZE
arch1.tar                31.00KiB
    arch1.tar/obj1       9.26KiB
    arch1.tar/obj2       9.26KiB
    arch1.tar/obj3       9.26KiB

$ ais archive bucket ais://bck/arch1.tar --template "obj{4..5}" --append
Archived "ais://bck/arch1.tar"

$ ais archive ls ais://bck/arch1.tar
NAME                     SIZE
arch1.tar                51.00KiB
    arch1.tar/obj1       9.26KiB
    arch1.tar/obj2       9.26KiB
    arch1.tar/obj3       9.26KiB
    arch1.tar/obj4       9.26KiB
    arch1.tar/obj5       9.26KiB
```

## List archived content

```console
NAME:
   ais archive ls - list archived content (supported formats: .tar, .tgz or .tar.gz, .zip, .tar.lz4)

USAGE:
   ais archive ls BUCKET[/SHARD_NAME] [command options]
```

List archived content as a tree with archive ("shard") name as a root and archived files as leaves.
Filenames are always sorted alphabetically.

### Options

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--props` | `string` | Comma-separated properties to return with object names | `"size"`
| `--all` | `bool` | Show all objects, including misplaced, duplicated, etc. | `false` |

### Examples

```console
$ ais archive ls ais://bck/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB
```

### Example: use '--prefix' that crosses shard boundary

For starters, we recursively archive all aistore docs:

```console
$ ais put docs ais://A.tar --archive -r
```

To list a virtual subdirectory _inside_ this newly created shard (e.g.):

```console
$ ais archive ls ais://nnn --prefix "A.tar/tutorials"
NAME                                             SIZE
    A.tar/tutorials/README.md                    561B
    A.tar/tutorials/etl/compute_md5.md           8.28KiB
    A.tar/tutorials/etl/etl_imagenet_pytorch.md  4.16KiB
    A.tar/tutorials/etl/etl_webdataset.md        3.97KiB
Listed: 4 names
````

or, same:

```console
$ ais ls ais://nnn --prefix "A.tar/tutorials" --archive
NAME                                             SIZE
    A.tar/tutorials/README.md                    561B
    A.tar/tutorials/etl/compute_md5.md           8.28KiB
    A.tar/tutorials/etl/etl_imagenet_pytorch.md  4.16KiB
    A.tar/tutorials/etl/etl_webdataset.md        3.97KiB
Listed: 4 names
```

## Get archived content

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
   ais get BUCKET[/OBJECT_NAME] [OUT_FILE|OUT_DIR|-] [command options]

OPTIONS:
   --offset value    object read offset; must be used together with '--length'; default formatting: IEC (use '--units' to override)
   --checksum        validate checksum
   --yes, -y         assume 'yes' to all questions
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

### Example: extract one file

```console
$ ais archive get ais://dst/A.tar.gz /tmp/w --archpath 111.ext1
GET 111.ext1 from ais://dst/A.tar.gz as "/tmp/w/111.ext1" (12.56KiB)

$ ls /tmp/w
111.ext1
```

Alternatively, use fully qualified name:

```console
$ ais archive get ais://dst/A.tar.gz/111.ext1 /tmp/w
```

### Example: extract one file using its fully-qualified name::

```console
$ ais archive get ais://nnn/A.tar/tutorials/README.md /tmp/out
```

### Example: extract all files from a single shard

Let's say, we have a certain shard in a certain bucket:

```console
$ ais ls ais://dst --archive
NAME                     SIZE
A.tar.gz                 5.18KiB
    A.tar.gz/111.ext1    12.56KiB
    A.tar.gz/222.ext1    12.56KiB
    A.tar.gz/333.ext2    12.56KiB
```

We can then go ahead to GET and extract it to local directory, e.g.:

```console
$ ais archive get ais://dst/A.tar.gz /tmp/www --extract
GET A.tar.gz from ais://dst as "/tmp/www/A.tar.gz" (5.18KiB) and extract to /tmp/www/A/

$ ls /tmp/www/A
111.ext1  222.ext1  333.ext2
```

But here's an alternative syntax to achieve the same:

```console
$ ais get ais://dst --archive --prefix A.tar.gz /tmp/www
```

or even:

```console
$ ais get ais://dst --archive --prefix A.tar.gz /tmp/www --progress --refresh 1 -y

GET 51 objects from ais://dst/tmp/ggg (total size 1.08MiB)
Objects:                   51/51 [==============================================================] 100 %
Total size:  1.08 MiB / 1.08 MiB [==============================================================] 100 %
```

The difference is that:

* in the first case we ask for a specific shard,
* while in the second (and third) we filter bucket's content using a certain prefix
* and the fact (the convention) that archived filenames are prefixed with their parent (shard) name.

### Example: extract all files from all shards (with a given prefix)

Let's say, there's a bucket `ais://dst` with a virtual directory `abc/` that in turn contains:

```console
$ ais ls ais://dst
NAME             SIZE
A.tar.gz         5.18KiB
B.tar.lz4        247.88KiB
C.tar.zip        4.15KiB
D.tar            2.00KiB
```

Next, we GET and extract them all in the respective sub-directories (note `--verbose` option):

```console
$ ais archive get ais://dst /tmp/w --prefix "" --extract -v

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
$ ais archive get ais://nnn --prefix A.tar/tutorials /tmp/out
GET 6 objects from ais://nnn/tmp/out (total size 17.81MiB) [Y/N]: y

$ ls -al /tmp/out/tutorials/
total 20
drwxr-x--- 4 root root 4096 May 13 20:05 ./
drwxr-xr-x 3 root root 4096 May 13 20:05 ../
drwxr-x--- 2 root root 4096 May 13 20:05 etl/
-rw-r--r-- 1 root root  561 May 13 20:05 README.md
drwxr-x--- 2 root root 4096 May 13 20:05 various/
```

## Get archived content: multiple selection

Generally, both single and multi-selection from a given source shard is realized using one of the following 4 (four) options:

```console
   --archpath value     extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                        see also: '--archregx'
   --archmime value     expected format (mime type) of an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
                        especially usable for shards with non-standard extensions
   --archregx value     string that specifies prefix, suffix, substring, WebDataset key, _or_ a general-purpose regular expression
                        to select possibly multiple matching archived files from a given shard;
                        is used in combination with '--archmode' ("matching mode") option
   --archmode value     enumerated "matching mode" that tells aistore how to handle '--archregx', one of:
                          * regexp - general purpose regular expression;
                          * prefix - matching filename starts with;
                          * suffix - matching filename ends with;
                          * substr - matching filename contains;
                          * wdskey - WebDataset key
                        example:
                          given a shard containing (subdir/aaa.jpg, subdir/aaa.json, subdir/bbb.jpg, subdir/bbb.json, ...)
                          and wdskey=subdir/aaa, aistore will match and return (subdir/aaa.jpg, subdir/aaa.json)
```

In particular, '--archregx' and '--archmode' pair defines multiple selection that can be further demonstrated on the following examples.

> But first, note that in all multi-selection cases, the result is (currently) invariably formatted as .TAR (that contains the aforementioned selection).

### Example: suffix match

Select all `*.jpeg` files from a given shard and return them all as 111.tar:

```console
$ ais archive get ais://abc/trunk-0123.tar 111.tar --archregx=jpeg --archmode=suffix
```

### Example: [WebDataset](https://github.com/webdataset/webdataset) key

Select all files that have a given [WebDataset](https://github.com/webdataset/webdataset) key; return the result as 222.tar:

```console
$ ais archive get ais://abc/trunk-0123.tar 222.tar --archregx=file45 --archmode=wdskey
```

### Example: prefix match

Similar to the above except that in this case '--archregx' value specifies virtual subdirectory inside a given named shard:

```console
$ ais archive get ais://abc/trunk-0123.tar 333.tar --archregx=subdir/ --archmode=prefix
```

## Generate shards

`ais archive gen-shards "BUCKET/TEMPLATE.EXT"`

Put randomly generated shards that can be used for dSort testing.
The `TEMPLATE` must be bash-like brace expansion (see examples) and `.EXT` must be one of: `.tar`, `.tar.gz`.

**Warning**: Remember to always quote the argument (`"..."`) otherwise the brace expansion will happen in terminal.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--fsize` | `string` | Single file size inside the shard, can end with size suffix (k, MB, GiB, ...) | `1024`  (`1KB`)|
| `--fcount` | `int` | Number of files inside single shard | `5` |
| `--fext` | `string` |  Comma-separated list of file extensions (default ".test"), e.g.: --fext '.mp3,.json,.cls' | `.test` |
| `--cleanup` | `bool` | When set, the old bucket will be deleted and created again | `false` |
| `--num-workers` | `int` | Limits the number of shards created concurrently | `10` |

### Examples

#### Generate shards with varying numbers of files and file sizes

Generate 10 shards each containing 100 files of size 256KB and put them inside `ais://dsort-testing` bucket (creates it if it does not exist).
Shards will be named: `shard-0.tar`, `shard-1.tar`, ..., `shard-9.tar`.

```console
$ ais archive gen-shards "ais://dsort-testing/shard-{0..9}.tar" --fsize 262144 --fcount 100
Shards created: 10/10 [==============================================================] 100 %
$ ais ls ais://dsort-testing
NAME		SIZE		VERSION
shard-0.tar	25.05MiB	1
shard-1.tar	25.05MiB	1
shard-2.tar	25.05MiB	1
shard-3.tar	25.05MiB	1
shard-4.tar	25.05MiB	1
shard-5.tar	25.05MiB	1
shard-6.tar	25.05MiB	1
shard-7.tar	25.05MiB	1
shard-8.tar	25.05MiB	1
shard-9.tar	25.05MiB	1
```

#### Generate shards using custom naming template

Generates 100 shards each containing 5 files of size 256KB and put them inside `dsort-testing` bucket.
Shards will be compressed and named: `super_shard_000_last.tgz`, `super_shard_001_last.tgz`, ..., `super_shard_099_last.tgz`

```console
$ ais archive gen-shards "ais://dsort-testing/super_shard_{000..099}_last.tar" --fsize 262144 --cleanup
Shards created: 100/100 [==============================================================] 100 %
$ ais ls ais://dsort-testing
NAME				SIZE	VERSION
super_shard_000_last.tgz	1.25MiB	1
super_shard_001_last.tgz	1.25MiB	1
super_shard_002_last.tgz	1.25MiB	1
super_shard_003_last.tgz	1.25MiB	1
super_shard_004_last.tgz	1.25MiB	1
super_shard_005_last.tgz	1.25MiB	1
super_shard_006_last.tgz	1.25MiB	1
super_shard_007_last.tgz	1.25MiB	1
...
```

#### Multi-extension example


```console
$ ais archive gen-shards 'ais://nnn/shard-{01..99}.tar' -fext ".mp3,  .json,  .cls"

$ ais archive ls ais://nnn | head -n 20
NAME                                             SIZE
shard-01.tar                                     23.50KiB
    shard-01.tar/541701ae863f76d0f7e0-0.cls      1.00KiB
    shard-01.tar/541701ae863f76d0f7e0-0.json     1.00KiB
    shard-01.tar/541701ae863f76d0f7e0-0.mp3      1.00KiB
    shard-01.tar/8f8c5fa2934c90138833-1.cls      1.00KiB
    shard-01.tar/8f8c5fa2934c90138833-1.json     1.00KiB
    shard-01.tar/8f8c5fa2934c90138833-1.mp3      1.00KiB
    shard-01.tar/9a42bd12d810d890ea86-3.cls      1.00KiB
    shard-01.tar/9a42bd12d810d890ea86-3.json     1.00KiB
    shard-01.tar/9a42bd12d810d890ea86-3.mp3      1.00KiB
    shard-01.tar/c5bd7c7a34e12ebf3ad3-2.cls      1.00KiB
    shard-01.tar/c5bd7c7a34e12ebf3ad3-2.json     1.00KiB
    shard-01.tar/c5bd7c7a34e12ebf3ad3-2.mp3      1.00KiB
    shard-01.tar/f13522533ecafbad4fe5-4.cls      1.00KiB
    shard-01.tar/f13522533ecafbad4fe5-4.json     1.00KiB
    shard-01.tar/f13522533ecafbad4fe5-4.mp3      1.00KiB
shard-02.tar                                     23.50KiB
    shard-02.tar/095e6ae644ff4fd1778b-7.cls      1.00KiB
    shard-02.tar/095e6ae644ff4fd1778b-7.json     1.00KiB
...
```
