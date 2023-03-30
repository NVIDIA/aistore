---
layout: post
title: BUCKET
permalink: /docs/cli/bucket
redirect_from:
 - /cli/bucket.md/
 - /docs/cli/bucket.md/
---

# CLI Reference for Buckets
This section lists operations on *buckets* using the AIS CLI, with `ais bucket`.
For types of supported buckets (AIS, Cloud, backend, etc.) and many more examples, please see the [in-depth overview of buckets](/docs/bucket.md).

## Table of Contents
- [Create bucket](#create-bucket)
- [Delete bucket](#delete-bucket)
- [List buckets](#list-buckets)
- [List objects](#list-objects)
- [Evict remote bucket](#evict-remote-bucket)
- [Move or Rename a bucket](#move-or-rename-a-bucket)
- [Copy bucket](#copy-bucket)
- [Show bucket summary](#show-bucket-summary)
- [Start N-way Mirroring](#start-n-way-mirroring)
- [Start Erasure Coding](#start-erasure-coding)
- [Show bucket properties](#show-bucket-properties)
- [Set bucket properties](#set-bucket-properties)
- [Reset bucket properties to cluster defaults](#reset-bucket-properties-to-cluster-defaults)
- [Show bucket metadata](#show-bucket-metadata)

## Create bucket

`ais create BUCKET [BUCKET...]`

Create bucket(s).

### Examples

#### Create AIS bucket

Create buckets `bucket_name1` and `bucket_name2`, both with AIS provider.

```console
$ ais create ais://bucket_name1 ais://bucket_name2
"ais://bucket_name1" bucket created
"ais://bucket_name2" bucket created
```

#### Create AIS bucket in local namespace

Create bucket `bucket_name` in `ml` namespace.

```console
$ ais create ais://#ml/bucket_name
"ais://#ml/bucket_name" bucket created
```

#### Create bucket in remote AIS cluster

Create bucket `bucket_name` in global namespace of AIS remote cluster with `Bghort1l` UUID.

```console
$ ais create ais://@Bghort1l/bucket_name
"ais://@Bghort1l/bucket_name" bucket created
```

Create bucket `bucket_name` in `ml` namespace of AIS remote cluster with `Bghort1l` UUID.

```console
$ ais create ais://@Bghort1l#ml/bucket_name
"ais://@Bghort1l#ml/bucket_name" bucket created
```

#### Create bucket with custom properties

Create bucket `bucket_name` with custom properties specified.

```console
$ # Key-value format
$ ais create ais://@Bghort1l/bucket_name --props="mirror.enabled=true mirror.copies=2"
"ais://@Bghort1l/bucket_name" bucket created
$
$ # JSON format
$ ais create ais://@Bghort1l/bucket_name --props='{"versioning": {"enabled": true, "validate_warm_get": true}}'
"ais://@Bghort1l/bucket_name" bucket created
```

#### Create HDFS bucket

Create bucket `bucket_name` in HDFS backend with bucket pointing to `/yt8m` directory.
More info about HDFS buckets can be found [here](/docs/providers.md#hdfs-provider).

```console
$ ais create hdfs://bucket_name --props="extra.hdfs.ref_directory=/yt8m"
"hdfs://bucket_name" bucket created
```


#### Incorrect buckets creation

```console
$ ais create aws://bucket_name
Create bucket "aws://bucket_name" failed: creating a bucket for any of the cloud or HTTP providers is not supported
```

## Delete bucket

`ais bucket rm BUCKET [BUCKET...]`

Delete an ais bucket or buckets.

### Examples

#### Remove AIS buckets

Remove AIS buckets `bucket_name1` and `bucket_name2`.

```console
$ ais bucket rm ais://bucket_name1 ais://bucket_name2
"ais://bucket_name1" bucket destroyed
"ais://bucket_name2" bucket destroyed
```

#### Remove AIS bucket in local namespace

Remove bucket `bucket_name` from `ml` namespace.

```console
$ ais bucket rm ais://#ml/bucket_name
"ais://#ml/bucket_name" bucket destroyed
```

#### Remove bucket in remote AIS cluster

Remove bucket `bucket_name` from global namespace of AIS remote cluster with `Bghort1l` UUID.

```console
$ ais bucket rm ais://@Bghort1l/bucket_name
"ais://@Bghort1l/bucket_name" bucket destroyed
```

Remove bucket `bucket_name` from `ml` namespace of AIS remote cluster with `Bghort1l` UUID.

```console
$ ais bucket rm ais://@Bghort1l#ml/bucket_name
"ais://@Bghort1l#ml/bucket_name" bucket destroyed
```

#### Incorrect buckets removal

Removing remote buckets is not supported.

```console
$ ais bucket rm aws://bucket_name
Operation "destroy-bck" is not supported by "aws://bucket_name"
```

## List buckets

`ais ls [command options] [PROVIDER:]

can conveniently list buckets (with or without "summarizing" them - see `--summary`) amd objects.

Notice the optional `[//BUCKET_NAME]`. When there's no bucket, `ais ls` will list buckets. Otherwise, naturally it'll list objects.

### `ais ls --regex "ngn*"`

List all buckets matching the `ngn*` regex expression.

### `ais ls aws:` or (same) `ais ls s3`

List all _existing_ buckets for the specific provider.

### `ais ls aws --all` or (same) `ais ls s3: --all`

List absolutely all buckets that cluster can "see" inclduing those that are not necessarily **present** in the cluster.

### `ais ls ais://` or (same) `ais ls ais`

List all AIS buckets.

### `ais ls ais://#name`

List all buckets for the `ais` provider and `name` namespace.

### `ais ls ais://@uuid#namespace`

List all remote AIS buckets that have `uuid#namespace` namespace. Note that:

* the `uuid` must be the remote cluster UUID (or its alias)
* while the `namespace` is optional name of the remote namespace

As a rule, when a (logical) `#namespace` in the bucket's name is omitted we use the global namespace that always exists.

### Options

```console
$ ais ls --help
NAME:
   ais ls - (alias for "bucket ls") list buckets, objects in buckets, and files in objects formatted as archives

USAGE:
   ais ls [command options] PROVIDER:[//BUCKET_NAME]

OPTIONS:
   --all                depending on the context:
                        - all objects in a given bucket, including misplaced and copies, or
                        - all buckets, including accessible (visible) remote buckets that are _not present_ in the cluster
   --cached             list only those objects from a remote bucket that are present ("cached")
   --props value        comma-separated list of object properties including name, size, version, copies, and more; e.g.:
                        --props all
                        --props name,size,cached
                        --props "ec, copies, custom, location"
   --regex value        regular expression; use it to match either bucket names or objects in a given bucket, e.g.:
                        ais ls --regex "(m|n)"         - match buckets such as ais://nnn, s3://mmm, etc.;
                        ais ls ais://nnn --regex "^A"  - match object names starting with letter A
   --no-headers, -H     display tables without headers
   --no-footers         display tables without footers
   --summary            show bucket sizes and used capacity; applies _only_ to buckets and objects that are _present_ in the cluster
   --anonymous          list public-access Cloud buckets that may disallow certain operations (e.g., 'HEAD(bucket)')
   --units value        show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
                        raw - do not convert to (or from) human-readable format
   --help, -h           show help
```


## List objects

`ais ls` is one of those commands that only keeps growing, in terms of supported options and capabilities.

The command:

`ais ls [command options] PROVIDER:[//BUCKET_NAME]`

can conveniently list buckets (with or without "summarizing" them - see `--summary`) amd objects.

Notice the optional `[//BUCKET_NAME]`. When there's no bucket, `ais ls` will list buckets. Otherwise, naturally it'll list objects.


```console
$ ais ls --help
NAME:
   ais ls - (alias for "bucket ls") list buckets, objects in buckets, and files in objects formatted as archives

USAGE:
   ais ls [command options] PROVIDER:[//BUCKET_NAME]

OPTIONS:
   --all                depending on the context:
                        - all objects in a given bucket, including misplaced and copies, or
                        - all buckets, including accessible (visible) remote buckets that are _not present_ in the cluster
   --cached             list only those objects from a remote bucket that are present ("cached")
   --name-only          faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)
   --props value        comma-separated list of object properties including name, size, version, copies, and more; e.g.:
                        --props all
                        --props name,size,cached
                        --props "ec, copies, custom, location"
   --regex value        regular expression; use it to match either bucket names or objects in a given bucket, e.g.:
                        ais ls --regex "(m|n)"         - match buckets such as ais://nnn, s3://mmm, etc.;
                        ais ls ais://nnn --regex "^A"  - match object names starting with letter A
   --template value     template to match object names; may contain prefix with zero or more ranges (with optional steps and gaps), e.g.:
                        --template 'dir/subdir/'
                        --template 'shard-{1000..9999}.tar'
                        --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                        --template "prefix-{0010..9999..2}-suffix"
   --prefix value       list objects that start with the specified prefix, e.g.:
                        '--prefix a/b/c' - list virtual directory a/b/c and/or objects from the virtual directory
                        a/b that have their names (relative to this directory) starting with the letter c
   --page-size value    maximum number of names per page (0 - the maximum is defined by the corresponding backend) (default: 0)
   --paged              list objects page by page, one page at a time (see also '--page-size' and '--limit')
   --limit value        limit object name count (0 - unlimited) (default: 0)
   --show-unmatched     list objects that were not matched by regex and template
   --no-headers, -H     display tables without headers
   --no-footers         display tables without footers
   --max-pages value    display up to this number pages of bucket objects (default: 0)
   --start-after value  list bucket's content alphabetically starting with the first name _after_ the specified
   --summary            show bucket sizes and used capacity; applies _only_ to buckets and objects that are _present_ in the cluster
   --anonymous          list public-access Cloud buckets that may disallow certain operations (e.g., 'HEAD(bucket)')
   --archive            list archived content (see docs/archive.md for details)
   --units value        show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
                        raw - do not convert to (or from) human-readable format
   --help, -h           show help
```

### Options

comma-separated list of object properties including name, size, version, ##copies, EC data and parity info, custom props (default: "name,size")

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | regular expression to match and select items in question | `""` |
| `--template` | `string` | template for matching object names, e.g.: 'shard-{900..999}.tar' | `""` |
| `--prefix` | `string` | list objects matching a given prefix | `""` |
| `--page-size` | `int` | maximum number of names per page (0 - the maximum is defined by the corresponding backend) | `0` |
| `--props` | `string` | comma-separated list of object properties including name, size, version, copies, EC data and parity info, custom metadata, location, and more; to include all properties, type '--props all' (default: "name,size") | `"name,size"` |
| `--limit` | `int` | limit object name count (0 - unlimited) | `0` |
| `--show-unmatched` | `bool` | list objects that were not matched by regex and/or template | `false` |
| `--all` | `bool` | depending on context: all objects (including misplaced ones and copies) _or_ all buckets (including remote buckets that are not present in the cluster) | `false` |
| -no-headers, -H | `bool` | display tables without headers | `false` |
| --no-footers | `bool` | display tables without footers | `false` |
| `--paged` | `bool` | list objects page by page, one page at a time (see also '--page-size' and '--limit') | `false` |
| `--max-pages` | `int` | display up to this number pages of bucket objects (default: 0) | `0` |
| `--marker` | `string` | list bucket's content alphabetically starting with the first name _after_ the specified | `""` |
| `--start-after` | `string` | Object name (marker) after which the listing should start | `""` |
| `--cached` | `bool` | list only those objects from a remote bucket that are present ("cached") | `false` |
| `--anonymous` | `bool` | list public-access Cloud buckets that may disallow certain operations (e.g., `HEAD(bucket)`) | `false` |
| `--archive` | `bool` | list archived content | `false` |
| `--summary` | `bool` | show bucket sizes and used capacity; by default, applies only to the buckets that are _present_ in the cluster (use '--all' option to override) | `false` |
| `--bytes` | `bool` | show sizes in bytes (ie., do not convert to KiB, MiB, GiB, etc.) | `false` |
| `--name-only` | `bool` | fast request to retrieve only the names of objects in the bucket; if defined, all comma-separated fields in the `--props` flag will be ignored with only two exceptions: `name` and `status` | `false` |

### Examples

#### List AIS and Cloud buckets with all defaults

List objects in the AIS bucket `bucket_name`.

```console
$ ais ls ais://bucket_name
NAME		SIZE
shard-0.tar	16.00KiB
shard-1.tar	16.00KiB
...
```

List objects in the remote bucket `bucket_name`.

```console
ais ls aws://bucket_name
NAME		SIZE
shard-0.tar	16.00KiB
shard-1.tar	16.00KiB
...
```

#### Include all properties

```console
# ais ls gs://webdataset-abc --anonymous --props all
NAME                             SIZE          CHECKSUM                           ATIME   VERSION                 CACHED  TARGET URL            STATUS  COPIES
coco-train2014-seg-000000.tar    958.48MiB     bdb89d1b854040b6050319e80ef44dde           1657297128665686        no      http://aistore:8081   ok      0
coco-train2014-seg-000001.tar    958.47MiB     8b94939b7d166114498e794859fb472c           1657297129387272        no      http://aistore:8081   ok      0
coco-train2014-seg-000002.tar    958.47MiB     142a8e81f965f9bcafc8b04eda65a0ce           1657297129904067        no      http://aistore:8081   ok      0
coco-train2014-seg-000003.tar    958.22MiB     113024d5def81365cbb6c404c908efb1           1657297130555590        no      http://aistore:8081   ok      0
...
```


#### List bucket from AIS remote cluster

List objects in the bucket `bucket_name` and `ml` namespace contained on AIS remote cluster with `Bghort1l` UUID.

```console
$ ais ls ais://@Bghort1l#ml/bucket_name
NAME		SIZE		VERSION
shard-0.tar	16.00KiB	1
shard-1.tar	16.00KiB	1
...
```

#### With prefix

List objects which match given prefix.

```console
$ ais ls ais://bucket_name --prefix "shard-1"
NAME		SIZE		VERSION
shard-1.tar	16.00KiB	1
shard-10.tar	16.00KiB	1
```

#### List archive contect

```console
$ ais ls ais://abc/ --prefix log
NAME             SIZE
log.tar.gz      3.11KiB

$ ais ls ais://abc/ --prefix log --archive
NAME                                             SIZE
log.tar.gz                                       3.11KiB
    log2.tar.gz/t_2021-07-27_14-08-50.log        959B
    log2.tar.gz/t_2021-07-27_14-10-36.log        959B
    log2.tar.gz/t_2021-07-27_14-12-18.log        959B
    log2.tar.gz/t_2021-07-27_14-13-23.log        295B
    log2.tar.gz/t_2021-07-27_14-13-31.log        1.02KiB
    log2.tar.gz/t_2021-07-27_14-14-16.log        1.71KiB
    log2.tar.gz/t_2021-07-27_14-15-15.log        1.90KiB
```

#### List anonymously (i.e., list public-access Cloud bucket)

```console
$ ais ls gs://webdataset-abc --anonymous
NAME                             SIZE
coco-train2014-seg-000000.tar    958.48MiB
coco-train2014-seg-000001.tar    958.47MiB
coco-train2014-seg-000002.tar    958.47MiB
coco-train2014-seg-000003.tar    958.22MiB
coco-train2014-seg-000004.tar    958.56MiB
coco-train2014-seg-000005.tar    958.19MiB
...
```

## Evict remote bucket

`ais bucket evict BUCKET`

Evict a [remote bucket](/docs/bucket.md#remote-bucket). It also resets the properties of the bucket (if changed).
All data from the remote bucket stored in the cluster will be removed, and AIS will stop keeping track of the remote bucket.
Read more about this feature [here](/docs/bucket.md#evict-remote-bucket).

```console
$ ais bucket evict aws://abc
"aws://abc" bucket evicted

# Dry run: the cluster will not be modified
$ ais bucket evict --dry-run aws://abc
[DRY RUN] No modifications on the cluster
EVICT: "aws://abc"

# Only evict the remote bucket's data (AIS will retain the bucket's metadata)
$ ais bucket evict --keep-md aws://abc
"aws://abc" bucket evicted
```

Here's a fuller example that lists remote bucket and then reads and evicts a selected object:

```console
$ ais ls gs://wrQkliptRt
NAME             SIZE
TDXBNBEZNl.tar   8.50KiB
qFpwOOifUe.tar   8.50KiB
thmdpZXetG.tar   8.50KiB

$ ais get gcp://wrQkliptRt/qFpwOOifUe.tar /tmp/qFpwOOifUe.tar
GET "qFpwOOifUe.tar" from bucket "gcp://wrQkliptRt" as "/tmp/qFpwOOifUe.tar" [8.50KiB]

$ ais ls gs://wrQkliptRt --props all
NAME             SIZE            CHECKSUM                                ATIME                   VERSION                 CACHED  STATUS  COPIES
TDXBNBEZNl.tar   8.50KiB         33345a69bade096a30abd42058da4537                                1622133976984266        no      ok      0
qFpwOOifUe.tar   8.50KiB         47dd59e41f6b7723                        28 May 21 12:02 PDT     1622133846120151        yes     ok      1
thmdpZXetG.tar   8.50KiB         cfe0c386e91daa1571d6a659f49b1408                                1622137609269706        no      ok      0

$ ais bucket evict gcp://wrQkliptRt
"gcp://wrQkliptRt" bucket evicted

$ ais ls gs://wrQkliptRt --props all
NAME             SIZE            CHECKSUM                                ATIME   VERSION                 CACHED  STATUS  COPIES
TDXBNBEZNl.tar   8.50KiB         33345a69bade096a30abd42058da4537                1622133976984266        no      ok      0
qFpwOOifUe.tar   8.50KiB         8b5919c0850a07d931c3c46ed9101eab                1622133846120151        no      ok      0
thmdpZXetG.tar   8.50KiB         cfe0c386e91daa1571d6a659f49b1408                1622137609269706        no      ok      0
```

> Note: When an [HDFS bucket](/docs/providers.md#hdfs-provider) is evicted, AIS will only remove objects stored in the cluster.
AIS will retain the bucket's metadata to allow the bucket to re-register later.

## Move or Rename a bucket

`ais bucket mv BUCKET NEW_BUCKET`

Move (ie. rename) an AIS bucket.
If the `NEW_BUCKET` already exists, the `mv` operation will not proceed.

> Cloud bucket move is not supported.

### Examples

#### Move AIS bucket

Move AIS bucket `bucket_name` to AIS bucket `new_bucket_name`.

```console
$ ais bucket mv ais://bucket_name ais://new_bucket_name
Moving bucket "ais://bucket_name" to "ais://new_bucket_name" in progress.
To check the status, run: ais show job xaction mvlb ais://new_bucket_name
```

## Copy bucket

`ais cp SRC_BUCKET DST_BUCKET`

Copy an existing bucket to a new bucket.

When the destination bucket is in the Cloud it must exist (and be writeable, of course).

For AIS (and remote AIS) buckets, on the other hand, the existence is optional. They get created on the fly, their properties copied from the source (`SRC_BUCKET`).

### Options

```console
$ ais cp --help
NAME:
   ais cp - (alias for "bucket cp") copy entire bucket or selected objects (to select, use '--list' or '--template')

USAGE:
   ais cp [command options] SRC_BUCKET DST_BUCKET

OPTIONS:
   --all             copy all objects from a remote bucket including those that are not present (not "cached") in the cluster
   --cont-on-err     keep running archiving xaction in presence of errors in a any given multi-object transaction
   --force, -f       force an action
   --dry-run         show total size of new objects without really creating them
   --prepend value   prefix to prepend to every copied object name, e.g.:
                     --prepend=abc   - prefix all copied object names with "abc"
                     --prepend=abc/  - copy objects into a virtual directory "abc" (note trailing filepath separator)
   --prefix value    copy objects that start with the specified prefix, e.g.:
                     '--prefix a/b/c' - copy virtual directory a/b/c and/or objects from the virtual directory
                     a/b that have their names (relative to this directory) starting with the letter c
   --list value      comma-separated list of object names, e.g.:
                     --list 'o1,o2,o3'
                     --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
   --template value  template to match object names; may contain prefix with zero or more ranges (with optional steps and gaps), e.g.:
                     --template 'dir/subdir/'
                     --template 'shard-{1000..9999}.tar'
                     --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                     --template "prefix-{0010..9999..2}-suffix"
   --progress        show progress bar(s) and progress of execution in real time
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --wait            wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --timeout value   maximum time to wait for a job to finish; if omitted wait forever or Ctrl-C;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --help, -h        show help
```

### Examples

#### Copy AIS bucket

Copy AIS bucket `src_bucket` to AIS bucket `dst_bucket`.

```console
$ ais cp ais://src_bucket ais://dst_bucket
Copying bucket "ais://bucket_name" to "ais://dst_bucket" in progress.
To check the status, run: ais show job xaction copy-bck ais://dst_bucket
```

#### Copy AIS bucket and wait until finish

The same as above, but wait until copying is finished.

```console
$ ais cp ais://src_bucket ais://dst_bucket --wait
```

#### Copy cloud bucket to another cloud bucket

Copy AWS bucket `src_bucket` to AWS bucket `dst_bucket`.

```console
# Make sure that both buckets exist.
$ ais ls aws://
AWS Buckets (2)
  aws://src_bucket
  aws://dst_bucket
$ ais cp aws://src_bucket aws://dst_bucket
Copying bucket "aws://src_bucket" to "aws://dst_bucket" in progress.
To check the status, run: ais show job xaction copy-bck aws://dst_bucket
```

#### Copy only selected objects

Copy objects `obj1.tar` and `obj1.info` from bucket `ais://bck1` to `ais://bck2`, and wait until the operation finishes.

```console
$ ais cp ais://bck1 ais://bck2 --list obj1.tar,obj1.info --wait
copying objects operation ("ais://bck1" => "ais://bck2") is in progress...
copying objects operation succeeded.
```

Copy object with pattern matching: copy `obj2`, `obj3`, and `obj4` from `ais://bck1` to `ais://bck2`.
Do not wait for the operation is done.

```console
$ ais cp ais://bck1 ais://bck2 --template "obj{2..4}"
copying objects operation ("ais://bck1" => "ais://bck2") is in progress...
To check the status, run: ais show job xaction copy-bck ais://bck2
```

## Show bucket summary

`ais storage summary [command options] PROVIDER:[//BUCKET_NAME] - show bucket sizes and the respective percentages of used capacity on a per-bucket basis

### Options

```console
NAME:
   ais storage summary - show bucket sizes and %% of used capacity on a per-bucket basis

USAGE:
   ais storage summary [command options] PROVIDER:[//BUCKET_NAME]

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --prefix value    for each bucket, select only those objects (names) that start with the specified prefix, e.g.:
                     '--prefix a/b/c' - sum-up sizes of the virtual directory a/b/c and objects from the virtual directory
                     a/b that have names (relative to this directory) starting with the letter c
   --cached          list only those objects from a remote bucket that are present ("cached")
   --all             all buckets, including accessible remote buckets that are not present in the cluster
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --verbose, -v     verbose
   --timeout value   maximum time to wait for a job to finish; if omitted wait forever or Ctrl-C;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --no-headers, -H  display tables without headers
   --help, -h        show help
```

If `BUCKET` is omitted, the command *applies* to all [AIS buckets](/docs/bucket.md#ais-bucket).

The output includes the total number of objects in a bucket, the bucket's size (bytes, megabytes, etc.), and the percentage of the total capacity used by the bucket.

A few additional words must be said about `--validate`. The option is provided to run integrity checks, namely: locations of objects, replicas, and EC slices in the bucket, the number of replicas (and whether this number agrees with the bucket configuration), and more.

> Location of each stored object must at any point in time correspond to the current cluster map and, within each storage target, to the target's [mountpaths](/docs/overview.md#terminology). A failure to abide by location rules is called *misplacement*; misplaced objects - if any - must be migrated to their proper locations via automated processes called `global rebalance` and `resilver`:

* [global rebalance and reslver](/docs/rebalance.md)
* [resilvering selected targets: advanced usage](/docs/resourcesvanced.md)

### Notes

1. `--validate` may take considerable time to execute (depending, of course, on sizes of the datasets in question and the capabilities of the underlying hardware);
2. non-zero *misplaced* objects in the (validated) output is a direct indication that the cluster requires rebalancing and/or resilvering;
3. `--fast=false` is another command line option that may also significantly increase execution time;
4. by default, `--fast` is set to `true`, which also means that bucket summary executes a *faster* logic (that may have a certain minor speed/accuracy trade-off);
5. to obtain the most precise results, run the command with `--fast=false` - and prepare to wait.

### Examples

```console
# 1. show summary for a specific bucket
$ ais bucket summary ais://abc
NAME             OBJECTS         SIZE ON DISK    USAGE(%)
ais://abc        10902           5.38GiB         1%

For min/avg/max object sizes, use `--fast=false`.
```

```console
# 2. "summarize" all buckets(*)
$ ais bucket summary
NAME             OBJECTS         SIZE ON DISK    USAGE(%)
ais://abc        10902           5.38GiB         1%
ais://nnn        49873           200.00MiB       0%
```

```console
# 3. summarize ais://abc to show min/avg/max object sizes and _apparent_ bucket sizes
###
###   Apparent bucket size is the sum(sizes of all objects in the bucket) - will
###   always be smaller than the actual disk usage. The difference will be even more
###   pronounced in presence of mirroring and/or erasure coding.
###   E.g., in a mirrored bucket configured for 3 replicas, the sum of all object
###   sizes will be more than 3 times smaller than the size on disk.
###
$ ais bucket summary ais://abc --fast=false
NAME             OBJECTS         OBJECT SIZE (min, avg, max)             SIZE (sum object sizes)   USAGE(%)
ais://abc        10902           1.07KiB    515.01KiB  1023.51KiB        5.35GiB                   1%
```

## Start N-way Mirroring

`ais start mirror BUCKET --copies <value>`

Start an extended action to bring a given bucket to a certain redundancy level (`value` copies). Read more about this feature [here](/docs/storage_svcs.md#n-way-mirror).

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--copies` | `int` | Number of copies | `1` |

## Start Erasure Coding

`ais ec-encode BUCKET --data-slices <value> --parity-slices <value>`

Start an extended action that enables data protection for a given bucket and encodes all its objects.
Erasure coding must be disabled for the bucket prior to running `ec-encode` extended action.
Read more about this feature [here](/docs/storage_svcs.md#erasure-coding).

### Options

| Flag | Type | Description |
| --- | --- | --- |
| `--data-slices`, `--data`, `-d` | `int` | Number of data slices |
| `--parity-slices`, `--parity`, `-p` | `int` | Number of parity slices |

All options are required and must be greater than `0`.

## Show bucket properties

Overall, the topic called "bucket properties" is rather involved and includes sub-topics "bucket property inhertance" and "cluster-wide global defaults". For background, please first see:

* [Default Bucket Properties](/docs/bucket.md#default-bucket-properties)
* [Inherited Bucket Properties and LRU](/docs/bucket.md#inherited-bucket-properties-and-lru)
* [Backend Provider](/docs/bucket.md#backend-provider)
* [Global cluster-wide configuration](/docs/configuration.md#cluster-and-node-configuration).

Now, as far as CLI, run the following to list [properties](/docs/bucket.md#properties-and-options) of the specified bucket.
By default, a certain compact form of bucket props sections is presented.

`ais bucket props show BUCKET [PROP_PREFIX]`

When `PROP_PREFIX` is set, only props that start with `PROP_PREFIX` will be displayed.
Useful `PROP_PREFIX` are: `access, checksum, ec, lru, mirror, provider, versioning`.

> Note: Like many other `ais show` commands, `ais show bucket` is aliased to `ais bucket show` for ease of use.
> Both of these commands are used interchangeably throughout the documentation.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json` | `bool` | Output in JSON format | `false` |
| `--compact`, `-c` | `bool` | Show list of properties in compact human-readable mode | `false` |

### Examples

#### Show bucket props with provided section

Show only `lru` section of bucket props for `bucket_name` bucket.

```console
$ ais bucket props show ais://bucket_name --compact
PROPERTY	 VALUE
access		 GET,PUT,DELETE,HEAD,ColdGET
checksum	 Type: xxhash | Validate: ColdGET
created		 2020-04-08T16:20:12-08:00
ec		 Disabled
lru		 Watermarks: 75%/90% | Do not evict time: 120m | OOS: 95%
mirror		 Disabled
provider	 ais
versioning	 Enabled | Validate on WarmGET: no
$ ais bucket props show ais://bucket_name lru --compact
PROPERTY	 VALUE
lru		 Watermarks: 75%/90% | Do not evict time: 120m | OOS: 95%
$ ais bucket props show bucket_name lru
PROPERTY		 VALUE
lru.capacity_upd_time	 10m
lru.dont_evict_time	 120m
lru.enabled		 true
lru.highwm		 90
lru.lowwm		 75
lru.out_of_space	 95
```

## Set bucket properties

`ais bucket props set [OPTIONS] BUCKET JSON_SPECIFICATION|KEY=VALUE [KEY=VALUE...]`

Set bucket properties.
For the available options, see [bucket-properties](/docs/bucket.md#bucket-properties).

If JSON_SPECIFICATION is used, **all** properties of the bucket are set based on the values in the JSON object.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--force` | `bool` | Ignore non-critical errors | `false` |

When JSON specification is not used, some properties support user-friendly aliases:

| Property | Value alias | Description |
| --- | --- | --- |
| access | `ro` | Disables bucket modifications: denies PUT, DELETE, and ColdGET requests |
| access | `rw` | Enables object modifications: allows PUT, DELETE, and ColdGET requests |
| access | `su` | Enables full access: all `rw` permissions, bucket deletion, and changing bucket permissions |

### Examples

#### Enable mirroring for a bucket

Set the `mirror.enabled` and `mirror.copies` properties to `true` and `2` respectively, for the bucket `bucket_name`

```console
$ ais bucket props set ais://bucket_name 'mirror.enabled=true' 'mirror.copies=2'
Bucket props successfully updated
"mirror.enabled" set to:"true" (was:"false")
```

#### Make a bucket read-only

Set read-only access to the bucket `bucket_name`.
All PUT and DELETE requests will fail.

```console
$ ais bucket props set ais://bucket_name 'access=ro'
Bucket props successfully updated
"access" set to:"GET,HEAD-OBJECT,HEAD-BUCKET,LIST-OBJECTS" (was:"<PREV_ACCESS_LIST>")
```

#### Configure custom AWS S3 endpoint

When a bucket is hosted by an S3 compliant backend (such as, e.g., minio), we may want to specify an alternative S3 endpoint,
so that AIS nodes use it when reading, writing, listing, and generally, performing all operations on remote S3 bucket(s).

Globally, S3 endpoint can be overridden for _all_ S3 buckets via "S3_ENDPOINT" environment.
If you decide to make the change, you may need to restart AIS cluster while making sure that "S3_ENDPOINT" is available for the AIS nodes
when they are starting up.

But it can be also be done - and will take precedence over the global setting - on a per-bucket basis.

Here are some examples:

```console
# Let's say, there exists a bucket called s3://abc:
$ ais ls s3://abc
NAME             SIZE
README.md        8.96KiB

# First, we override empty the endpoint property in the bucket's configuration.
# To see that a non-empty value *applies* and works, we will use the default AWS S3 endpoint: https://s3.amazonaws.com
$ ais bucket props set s3://abc extra.aws.endpoint=s3.amazonaws.com
Bucket "aws://abc": property "extra.aws.endpoint=s3.amazonaws.com", nothing to do
$ ais ls s3://abc
NAME             SIZE
README.md        8.96KiB

# Second, set the endpoint=foo (or, it could be any other invalid value), and observe that the bucket becomes unreachable:
$ ais bucket props set s3://abc extra.aws.endpoint=foo
Bucket props successfully updated
"extra.aws.endpoint" set to: "foo" (was: "s3.amazonaws.com")
$ ais ls s3://abc
RequestError: send request failed: dial tcp: lookup abc.foo: no such host

# Finally, revert the endpoint back to empty, and check that the bucket is visible again:
$ ais bucket props set s3://abc extra.aws.endpoint=""
Bucket props successfully updated
"extra.aws.endpoint" set to: "" (was: "foo")
$ ais ls s3://abc
NAME             SIZE
README.md        8.96KiB
```

> Global `export S3_ENDPOINT=...` override is static and readonly. Use it with extreme caution as it applies to all buckets.

> On the other hand, for any given `s3://bucket` its S3 endpoint can be set, unset, and otherwise changed at any time - at runtime. As shown above.


#### Connect/Disconnect AIS bucket to/from cloud bucket

Set backend bucket for AIS bucket `bucket_name` to the GCP cloud bucket `cloud_bucket`.
Once the backend bucket is set, operations (get, put, list, etc.) with `ais://bucket_name` will be exactly as we would do with `gcp://cloud_bucket`.
It's like a symlink to a cloud bucket.
The only difference is that all objects will be cached into `ais://bucket_name` (and reflected in the cloud as well) instead of `gcp://cloud_bucket`.

```console
$ ais bucket props set ais://bucket_name backend_bck=gcp://cloud_bucket
Bucket props successfully updated
"backend_bck.name" set to: "cloud_bucket" (was: "")
"backend_bck.provider" set to: "gcp" (was: "")
```

To disconnect cloud bucket do:

```console
$ ais bucket props set ais://bucket_name backend_bck=none
Bucket props successfully updated
"backend_bck.name" set to: "" (was: "cloud_bucket")
"backend_bck.provider" set to: "" (was: "gcp")
```

#### Ignore non-critical errors

To create an erasure-encoded bucket or enable EC for an existing bucket, AIS requires at least `ec.data_slices + ec.parity_slices + 1` targets.
At the same time, for small objects (size is less than `ec.objsize_limit`) it is sufficient to have only `ec.parity_slices + 1` targets.
Option `--force` allows creating erasure-encoded buckets when the number of targets is not enough but the number exceeds `ec.parity_slices`.

Note that if the number of targets is less than `ec.data_slices + ec.parity_slices + 1`, the cluster accepts only objects smaller than `ec.objsize_limit`.
Bigger objects are rejected on PUT.

In examples a cluster with 6 targets is used:

```console
$ # Creating a bucket
$ ais create ais://bck --bucket-props "ec.enabled=true ec.data_slices=6 ec.parity_slices=4"
Create bucket "ais://bck" failed: EC config (6 data, 4 parity) slices requires at least 11 targets (have 6)
$
$ ais create ais://bck --bucket-props "ec.enabled=true ec.data_slices=6 ec.parity_slices=4" --force
"ais://bck" bucket created
$
$ # If the number of targets is less than or equal to ec.parity_slices even `--force` does not help
$
$ ais bucket props set ais://bck ec.enabled true ec.data_slices 6 ec.parity_slices 8
EC config (6 data, 8 parity)slices requires at least 15 targets (have 6). To show bucket properties, run "ais show bucket BUCKET -v".
$
$ ais bucket props set ais://bck ec.enabled true ec.data_slices 6 ec.parity_slices 8 --force
EC config (6 data, 8 parity)slices requires at least 15 targets (have 6). To show bucket properties, run "ais show bucket BUCKET -v".
$
$ # Use force to enable EC if the number of target is sufficient to keep `ec.parity_slices+1` replicas
$
$ ais bucket props set ais://bck ec.enabled true ec.data_slices 6 ec.parity_slices 4
EC config (6 data, 8 parity)slices requires at least 11 targets (have 6). To show bucket properties, run "ais show bucket BUCKET -v".
$
$ ais bucket props set ais://bck ec.enabled true ec.data_slices 6 ec.parity_slices 4 --force
Bucket props successfully updated
"ec.enabled" set to: "true" (was: "false")
"ec.parity_slices" set to: "4" (was: "2")
```

Once erasure encoding is enabled for a bucket, the number of data and parity slices cannot be modified.
The minimum object size `ec.objsize_limit` can be changed on the fly.
To avoid accidental modification when EC for a bucket is enabled, the option `--force` must be used.

```console
$ ais bucket props set ais://bck ec.enabled true
Bucket props successfully updated
"ec.enabled" set to: "true" (was: "false")
$
$ ais bucket props set ais://bck ec.objsize_limit 320000
P[dBbfp8080]: once enabled, EC configuration can be only disabled but cannot change. To show bucket properties, run "ais show bucket BUCKET -v".
$
$ ais bucket props set ais://bck ec.objsize_limit 320000 --force
Bucket props successfully updated
"ec.objsize_limit" set to:"320000" (was:"262144")
```

#### Set bucket properties with JSON

Set **all** bucket properties for `bucket_name` bucket based on the provided JSON specification.

```bash
$ ais bucket props set ais://bucket_name '{
    "provider": "ais",
    "versioning": {
      "enabled": true,
      "validate_warm_get": false
    },
    "checksum": {
      "type": "xxhash",
      "validate_cold_get": true,
      "validate_warm_get": false,
      "validate_obj_move": false,
      "enable_read_range": false
    },
    "lru": {
      "lowwm": 20,
      "highwm": 80,
      "out_of_space": 90,
      "dont_evict_time": "20m",
      "capacity_upd_time": "1m",
      "enabled": true
    },
    "mirror": {
      "copies": 2,
      "burst_buffer": 512,
      "enabled": false
    },
    "ec": {
        "objsize_limit": 256000,
        "data_slices": 2,
        "parity_slices": 2,
        "enabled": true
    },
    "access": "255"
}'
Bucket props successfully updated
```

```console
$ ais show bucket ais://bucket_name --compact
PROPERTY	 VALUE
access		 GET,PUT,DELETE,HEAD,ColdGET
checksum	 Type: xxhash | Validate: ColdGET
created		2020-04-08T16:20:12-08:00
ec		 2:2 (250KiB)
lru		 Watermarks: 20%/80% | Do not evict time: 120m | OOS: 90%
mirror		 Disabled
provider	 ais
versioning	 Enabled | Validate on WarmGET: no
```

If not all properties are mentioned in the JSON, the missing ones are set to zero values (empty / `false` / `nil`):

```bash
$ ais bucket props reset ais://bucket_name
Bucket props successfully reset
$ ais bucket props set ais://bucket_name '{
  "mirror": {
    "enabled": true,
    "copies": 2
  },
  "versioning": {
    "enabled": true,
    "validate_warm_get": true
  }
}'
Bucket props successfully updated
"versioning.validate_warm_get" set to: "true" (was: "false")
"mirror.enabled" set to: "true" (was: "false")
$ ais show bucket bucket_name --compact
PROPERTY	 VALUE
access		 GET,PUT,DELETE,HEAD,ColdGET
checksum	 Type: xxhash | Validate: ColdGET
created		2020-04-08T16:20:12-08:00
ec		 Disabled
lru		 Watermarks: 75%/90% | Do not evict time: 120m | OOS: 95%
mirror		 2 copies
provider	 ais
versioning	 Enabled | Validate on WarmGET: yes
```

## Reset bucket properties to cluster defaults

`ais bucket props reset BUCKET`

Reset bucket properties to [cluster defaults](/docs/resourcesnfig.md).

### Examples

```console
$ ais bucket props reset bucket_name
Bucket props successfully reset
```

## Show bucket metadata

`ais show cluster bmd`

Show bucket metadata (BMD).

### Examples

```console
$ ais show cluster bmd
PROVIDER  NAMESPACE  NAME        BACKEND  COPIES  EC(D/P, minsize)  CREATED
ais                  test                 2                         25 Mar 21 18:28 PDT
ais                  validation                                     25 Mar 21 18:29 PDT
ais                  train                                          25 Mar 21 18:28 PDT

Version:        9
UUID:           jcUfFDyTN
```
