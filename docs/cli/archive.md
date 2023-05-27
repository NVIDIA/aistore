---
layout: post
title: BUCKET
permalink: /docs/cli/archive
redirect_from:
 - /cli/archive.md/
 - /docs/cli/archive.md/
---

# When objects are, in fact, archives

In this document: commands to read, write, and list *archives* - objects formatted as TAR, TGZ, ZIP, etc. For the most recently updated archival types that AIS supports, please refer to [this source](/cmn/cos/archive.go).

The corresponding subset of CLI commands starts with `ais archive`, from where you can `<TAB-TAB>` to the actual (reading, writing, listing) operation.

See also:

> [Append file to archive](/docs/cli/object.md#append-file-to-archive)
> [Archive multiple objects](/docs/cli/object.md#archive-multiple-objects)

## Table of Contents
- [Archive multiple objects](#archive-multiple-objects)
- [List archive content](#list-archive-content)
- [Append file to archive](#append-file-to-archive)

## Archive multiple objects

Archive multiple objects from the source bucket.

```console
$ ais archive put --help
NAME:
   ais archive put - put multi-object (.tar, .tgz or .tar.gz, .zip, .tar.lz4) archive

USAGE:
   ais archive put [command options] SRC_BUCKET DST_BUCKET/OBJECT_NAME

OPTIONS:
   --template value   template to match object names; may contain prefix with zero or more ranges (with optional steps and gaps), e.g.:
                      --template 'dir/subdir/'
                      --template 'shard-{1000..9999}.tar'
                      --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                      --template "prefix-{0010..9999..2}-suffix"
   --list value       comma-separated list of object names, e.g.:
                      --list 'o1,o2,o3'
                      --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
   --include-src-bck  prefix names of archived objects with the source bucket name
   --append           if destination object ("archive", "shard") already exists, append to it
                      (instead of creating a new one)
   --cont-on-err      keep running archiving xaction in presence of errors in a any given multi-object transaction
```

The operation accepts either an explicitly defined *list* or template-defined *range* of object names (to archive).

As such, `archive put` is one of the supported [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges).

Also note that `ais put` command with its `--archive` option provides an alternative way to archive multiple objects:

* [`ais put BUCKET/OBJECT --archive`](/docs/cli/object.md##archive-multiple-objects)

For the most recently updated list of supported archival formats, please see:

* [this source](https://github.com/NVIDIA/aistore/blob/master/cmn/cos/archive.go).

### Examples

1. Archive list of objects from a given bucket:

```console
$ ais archive put ais://bck/arch.tar --list obj1,obj2
Archiving "ais://bck/arch.tar" ...
```

Resulting `ais://bck/arch.tar` contains objects `ais://bck/obj1` and `ais://bck/obj2`.

2. Archive objects from a different bucket, use template (range):

```console
$ ais archive put ais://src ais://dst/arch.tar --template "obj-{0..9}"

Archiving "ais://dst/arch.tar" ...
```

`ais://dst/arch.tar` now contains 10 objects from bucket `ais://src`: `ais://src/obj-0`, `ais://src/obj-1` ... `ais://src/obj-9`.

3. Archive 3 objects and then append 2 more:

```console
$ ais archive put ais://bck/arch1.tar --template "obj{1..3}"
Archived "ais://bck/arch1.tar" ...
$ ais archive ls ais://bck/arch1.tar
NAME                     SIZE
arch1.tar                31.00KiB
    arch1.tar/obj1       9.26KiB
    arch1.tar/obj2       9.26KiB
    arch1.tar/obj3       9.26KiB

$ ais archive put ais://bck/arch1.tar --template "obj{4..5}" --append
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

## List archive content

`ais archive ls BUCKET/OBJECT`

Display an archive content as a tree, where the root is the archive name and leaves are files inside the archive.
The filenames are always sorted alphabetically.

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

## Append file to archive

Add a file to an existing archive.

### Options

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--archpath` | `string` | Path inside the archive for the new file | `""`

**NOTE:** the option `--archpath` cannot be omitted (MUST be specified).

### Example 1

```console
# contents _before_:
$ ais archive ls ais://bck/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB

# Do append:
$ ais archive /tmp/obj1.bin ais://bck/arch.tar --archpath bin/obj1
APPEND "/tmp/obj1.bin" to object "ais://bck/arch.tar[/bin/obj1]"

# contents _after_:
$ ais archive ls ais://bck/arch.tar
NAME                    SIZE
arch.tar                6KiB
    arch.tar/bin/obj1   2.KiB
    arch.tar/obj1       1.0KiB
    arch.tar/obj2       1.0KiB
```

### Example 2

```console
# contents _before_:

$ ais archive ls ais://nnn/shard-2.tar
NAME                                             SIZE
shard-2.tar                                      5.50KiB
    shard-2.tar/0379f37cbb0415e7eaea-3.test      1.00KiB
    shard-2.tar/504c563d14852368575b-5.test      1.00KiB
    shard-2.tar/c7bcb7014568b5e7d13b-4.test      1.00KiB

# Do append
# Note that --archpath can specify fully qualified name of the destination

$ ais archive append LICENSE ais://nnn/shard-2.tar --archpath shard-2.tar/license.test
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
