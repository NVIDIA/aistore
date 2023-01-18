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

`ais archive create BUCKET/OBJECT [command options]`

Archive a list or range of existing objects. Name the resulting (`.tar`, `.tar.gz`, `.zip`, `.msgpack`) archive `BUCKET/OBJECT`.

The operation accepts either an explicitly defined *list* or template-defined *range* of object names (to archive).

As such, `archive create` is one of the supported [multi-object operations](/docs/cli/object.md#operations-on-lists-and-ranges).

Also note that `ais put` command with its `--archive` option provides an alternative way to archive multiple objects:

* [`ais object put BUCKET/OBJECT --archive`](/docs/cli/object.md##archive-multiple-objects)

For the most recently updated list of supported archival formats, please see:

* [this source](https://github.com/NVIDIA/aistore/blob/master/cmn/cos/archive.go).

### Options

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--source-bck` | `string` | Bucket that contains the source objects. If not set, source and destination buckets are the same | `""` |
| `--template` | `string` | The object name template with optional range parts, e.g.: 'shard-{900..999}.tar' | `""` |
| `--list` | `string` | Comma separated list of objects for adding to archive | `""` |
| `--cleanup` | `bool` | delete or evict the source objects upon successful archiving | `false` |
| `--skip-misplaced` | `bool` | skip misplaced objects | `false` |
| `--include-bck` | `bool` | true - archive directory structure starts with bucket name, false - objects are put to the archive root | `false` |
| `--ignore-error` | `bool` | ignore error on soft failures like bucket already exists, bucket does not exist etc | `false` |
| `--append-to-arch` | `bool` | true - append to an archive if already exists, false - create a new archive | `false` |

The command must include either `--list` or `--template` option. Options `--list` and `--template` are mutually exclusive.

### Examples

Create an archive from a list of files of the same bucket:

```console
$ ais archive create ais://bck/arch.tar --list obj1,obj2
Creating archive "ais://bck/arch.tar"
```

The archive `ais://bck/arch.tar` contains objects `ais://bck/obj1` and `ais://bck/obj2`.

Create an archive using template:

```console
$ ais archive create ais://bck/arch.tar --source-bck ais://bck2 --template "obj-{0..9}"
Creating archive "ais://arch.tar"
```
The archive `ais://bck/arch.tar` contains 10 objects from bucket `ais://bck2`: `ais://bck2/obj-0`, `ais://bck2/obj-1` ... `ais://bck2/obj-9`.

Create an archive consisting of 3 objects and then append 2 more:

```console
$ ais archive create ais://bck/arch1.tar --template "obj{1..3}"
Creating archive "ais://bck/arch1.tar" ...
$ ais archive ls ais://bck/arch1.tar
NAME                     SIZE
arch1.tar                31.00KiB
    arch1.tar/obj1       9.26KiB
    arch1.tar/obj2       9.26KiB
    arch1.tar/obj3       9.26KiB
$ ais archive create ais://bck/arch1.tar --template "obj{4..5}" --append-to-arch
Created archive "ais://bck/arch1.tar"
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
The files are always are sorted in alphabetical order.

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
