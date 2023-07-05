---
layout: post
title: BUCKET
permalink: /docs/cli/archive
redirect_from:
 - /cli/archive.md/
 - /docs/cli/archive.md/
---

# When objects are, in fact, archives (shards)

In this document: commands to read, write, and list *archives* - objects formatted as TAR, TGZ, ZIP, etc. For the most recently updated archival types that AIS supports, please refer to [this source](/cmn/cos/archive.go).

The corresponding subset of CLI commands starts with `ais archive`, from where you can `<TAB-TAB>` to the actual (reading, writing, listing) operation.

```console
$ ais archive --help

NAME:
   ais archive - Archive multiple objects from a given bucket; archive local files and directories; list archived content

USAGE:
   ais archive command [command options] [arguments...]

COMMANDS:
   bucket      archive multiple objects from SRC_BUCKET as (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted shard
   put         archive a file, a directory, or multiple files and/or directories as
               (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object - aka "shard".
               Both APPEND (to an existing shard) and PUT (new version of the shard) variants are supported.
               Examples:
               - 'local-filename bucket/shard-00123.tar.lz4 --archpath name-in-archive' - append a file to a given shard and name it as specified;
               - 'src-dir bucket/shard-99999.zip -put' - one directory; iff the destination .zip doesn't exist create a new one;
               - '"sys, docs" ais://dst/CCC.tar --dry-run -y -r --archpath ggg/' - dry-run to recursively archive two directories.
               Tips:
               - use '--dry-run' option if in doubt;
               - to archive objects from a local or remote bucket, run 'ais archive bucket', see --help for details.
   get         get a shard, an archived file, or a range of bytes from the above;
               - use '--prefix' to get multiple objects in one shot (empty prefix for the entire bucket)
               - write the content locally with destination options including: filename, directory, STDOUT ('-')
   ls          list archived content (supported formats: .tar, .tgz or .tar.gz, .zip, .tar.lz4)
   gen-shards  generate random (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted objects ("shards"), e.g.:
               - gen-shards 'ais://bucket1/shard-{001..999}.tar' - write 999 random shards (default sizes) to ais://bucket1
               - gen-shards "gs://bucket2/shard-{01..20..2}.tgz" - 10 random gzipped tarfiles to Cloud bucket
               (notice quotation marks in both cases)
```

See also:

> [Append file to archive](/docs/cli/object.md#append-file-to-archive)
> [Archive multiple objects](/docs/cli/object.md#archive-multiple-objects)

## Table of Contents
- [Archive files and directories](#archive-files-and-directories)
- [Append files and directories to an existing archive](#append-files-and-directoriesto-an-existing-archive)
- [Archive multiple objects](#archive-multiple-objects)
- [List archived content](#list-archived-content)
- [Get archived content](#get-archived-content)

## Archive files and directories

Archive multiple objects from the source bucket.

```console
$ ais archive put --help
NAME:
   ais archive put - archive a file, a directory, or multiple files and/or directories as
     (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object - aka "shard".
     Both APPEND (to an existing shard) and PUT (a new version of the shard) are supported.
     Examples:
     - 'local-filename bucket/shard-00123.tar.lz4 --append --archpath name-in-archive' - append file to a given shard,
        optionally, rename it (inside archive) as specified;
     - 'local-filename bucket/shard-00123.tar.lz4 --append-or-put --archpath name-in-archive' - append file to a given shard if exists,
        otherwise, create a new shard (and name it shard-00123.tar.lz4, as specified);
     - 'src-dir bucket/shard-99999.zip -put' - one directory; iff the destination .zip doesn't exist create a new one;
     - '"sys, docs" ais://dst/CCC.tar --dry-run -y -r --archpath ggg/' - dry-run to recursively archive two directories.
     Tips:
     - use '--dry-run' option if in doubt;
     - to archive objects from a ais:// or remote bucket, run 'ais archive bucket', see --help for details.

USAGE:
   ais archive put [command options] [-|FILE|DIRECTORY[/PATTERN]] BUCKET/SHARD_NAME
```

The operation accepts either an explicitly defined *list* or template-defined *range* of object names (to archive).

As such, `archive put` is one of the supported [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges).

Also note that `ais put` command with its `--archive` option provides an alternative way to archive multiple objects:

* [`ais put BUCKET/OBJECT --archive`](/docs/cli/object.md##archive-multiple-objects)

For the most recently updated list of supported archival formats, please see:

* [this source](https://github.com/NVIDIA/aistore/blob/master/cmn/cos/archive.go).

## Append files and directories to an existing archive

APPEND operation provides for appending files to existing archives (shards). As such, APPEND is a variation of PUT (above) with additional **two boolean flags**:

| Name | Description |
| --- | --- |
| `--append` | add newly archived content to the destination object (\"archive\", \"shard\") that **must** exist |
| `--append-or-put` | **if** destination object (\"archive\", \"shard\") exists append to it, otherwise archive a new one |

### Examples

```console
# contents _before_:
$ ais archive ls ais://bck/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB

# Do append:
$ ais archive put /tmp/obj1.bin ais://bck/arch.tar --archpath bin/obj1
APPEND "/tmp/obj1.bin" to object "ais://bck/arch.tar[/bin/obj1]"

# contents _after_:
$ ais archive ls ais://bck/arch.tar
NAME                    SIZE
arch.tar                6KiB
    arch.tar/bin/obj1   2.KiB
    arch.tar/obj1       1.0KiB
    arch.tar/obj2       1.0KiB
```

```console
# contents _before_:

$ ais archive ls ais://nnn/shard-2.tar
NAME                                             SIZE
shard-2.tar                                      5.50KiB
    shard-2.tar/0379f37cbb0415e7eaea-3.test      1.00KiB
    shard-2.tar/504c563d14852368575b-5.test      1.00KiB
    shard-2.tar/c7bcb7014568b5e7d13b-4.test      1.00KiB

# Do append
# Note that `--archpath` can specify fully qualified name of the destination

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

```console
$ ais archive bucket --help
NAME:
   ais archive bucket - archive multiple objects from SRC_BUCKET as (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted shard

USAGE:
   ais archive bucket [command options] SRC_BUCKET DST_BUCKET/SHARD_NAME

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
   ais archive ls [command options] BUCKET[/SHARD_NAME]
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
   ais get [command options] BUCKET[/OBJECT_NAME] [OUT_FILE|OUT_DIR|-]

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
| `--cleanup` | `bool` | When set, the old bucket will be deleted and created again | `false` |
| `--conc` | `int` | Limits number of concurrent `PUT` requests and number of concurrent shards created | `10` |

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
