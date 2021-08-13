---
layout: post
title: BUCKET
permalink: /docs/cli/archive
redirect_from:
 - /cli/archive.md/
 - /docs/cli/archive.md/
---

# CLI Reference for Archives
This section lists operations on *archives* using AIS CLI, with `ais archive`.
For types of supported archives, please see [supported archive type](/cmn/cos/archive.go).

## Table of Contents
- [Create archive](#create-archive)
- [List archive content](#list-archive-content)
- [Append file to archive](#append-file-to-archive)

## Create archive

`ais archive create BUCKET/OBJECT`

Create an archive from existing objects.

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
| `--append-to-arch` | `bool` | true - append objects to an archive if it already exists, false - create new archive | `false` |

The command must include either `--list` or `--template` option. Options `--list` and `--template` are mutually exclusive.

### Examples

Create an archive from a list of files of the same bucket:

```
$ ais archive create ais://bck/arch.tar --list obj1,obj2
Creating archive "ais://bck/arch.tar"
```

The archive `ais://bck/arch.tar` contains objects `ais://bck/obj1` and `ais://bck/obj2`.

Create an archive using template:

```
$ais archive create ais://bck/arch.tar --source-bck ais://bck2 --template "obj-{0..9}"
Creating archive "ais://arch.tar"
```
The archive `ais://bck/arch.tar` contains 10 objects from bucket `ais://bck2`: `ais://bck2/obj-0`, `ais://bck2/obj-1` ... `ais://bck2/obj-9`.

Create an archive with 3 objects and append two more:

```
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

```
$ ais archive ls ais://bck/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB
```

## Append file to archive

Add a local file to an existing archive.

### Examples

### Options

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--archpath` | `string` | Path inside the archive for the new file | `""`

```
$ ais archive ls ais://bck/arch.tar
NAME                SIZE
arch.tar            4.5KiB
    arch.tar/obj1   1.0KiB
    arch.tar/obj2   1.0KiB

$ ais archive /tmp/obj1.bin ais://bck/arch.tar --archpath bin/obj1
APPEND "/tmp/obj1.bin" to object "ais://bck/arch.tar[/bin/obj1]"

$ ais archive ls ais://bck/arch.tar
NAME                    SIZE
arch.tar                6KiB
    arch.tar/bin/obj1   2.KiB
    arch.tar/obj1       1.0KiB
    arch.tar/obj2       1.0KiB
```

