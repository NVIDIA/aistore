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
- [List object names](#list-object-names)
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

`ais bucket create BUCKET [BUCKET...]`

Create bucket(s).

### Examples

#### Create AIS bucket

Create buckets `bucket_name1` and `bucket_name2`, both with AIS provider.

```console
$ ais bucket create ais://bucket_name1 ais://bucket_name2
"ais://bucket_name1" bucket created
"ais://bucket_name2" bucket created
```

#### Create AIS bucket in local namespace

Create bucket `bucket_name` in `ml` namespace.

```console
$ ais bucket create ais://#ml/bucket_name
"ais://#ml/bucket_name" bucket created
```

#### Create bucket in remote AIS cluster

Create bucket `bucket_name` in global namespace of AIS remote cluster with `Bghort1l` UUID.

```console
$ ais bucket create ais://@Bghort1l/bucket_name
"ais://@Bghort1l/bucket_name" bucket created
```

Create bucket `bucket_name` in `ml` namespace of AIS remote cluster with `Bghort1l` UUID.

```console
$ ais bucket create ais://@Bghort1l#ml/bucket_name
"ais://@Bghort1l#ml/bucket_name" bucket created
```

#### Create bucket with custom properties

Create bucket `bucket_name` with custom properties specified.

```console
$ # Key-value format
$ ais bucket create ais://@Bghort1l/bucket_name --props="mirror.enabled=true mirror.copies=2"
"ais://@Bghort1l/bucket_name" bucket created
$
$ # JSON format
$ ais bucket create ais://@Bghort1l/bucket_name --props='{"versioning": {"enabled": true, "validate_warm_get": true}}'
"ais://@Bghort1l/bucket_name" bucket created
```

#### Create HDFS bucket

Create bucket `bucket_name` in HDFS backend with bucket pointing to `/yt8m` directory.
More info about HDFS buckets can be found [here](/docs/providers.md#hdfs-provider).

```console
$ ais bucket create hdfs://bucket_name --props="extra.hdfs.ref_directory=/yt8m"
"hdfs://bucket_name" bucket created
```


#### Incorrect buckets creation

```console
$ ais bucket create aws://bucket_name
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

`ais bucket ls`

List all buckets.

`ais bucket ls --regex "ngn*"`

List all buckets matching the `ngn*` regex expression.

`ais bucket ls aws://` or `ais bucket ls ais://`

List all buckets for the specific provider.

`ais bucket ls ais://#name`

List all buckets for the `ais` provider and `name` namespace.

`ais bucket ls ais://@uuid#namespace`

List all buckets for the `ais` provider and `uuid#namespace` namespace.
`uuid` should be equal to remote cluster UUID and `namespace` is optional name of the remote namespace (if `namespace` not provided the global namespace will be used).

### Options

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | Pattern for matching bucket names | `""` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

## List objects

`ais ls <BUCKET>` (or, same, `ais bucket ls <BUCKET>`)

List all or some objects in `<BUCKET>`.

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
| -no-headers, --no-header, -H | `bool` | display tables without headers | `false` |
| --no-footers, --no-footer | `bool` | display tables without footers | `false` |
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
$ ais bucket ls ais://bucket_name
NAME		SIZE
shard-0.tar	16.00KiB
shard-1.tar	16.00KiB
...
```

List objects in the remote bucket `bucket_name`.

```console
ais bucket ls aws://bucket_name
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
$ ais bucket ls ais://@Bghort1l#ml/bucket_name
NAME		SIZE		VERSION
shard-0.tar	16.00KiB	1
shard-1.tar	16.00KiB	1
...
```

#### With prefix

List objects which match given prefix.

```console
$ ais bucket ls ais://bucket_name --prefix "shard-1"
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

$ ais object get gcp://wrQkliptRt/qFpwOOifUe.tar /tmp/qFpwOOifUe.tar
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

`ais bucket cp SRC_BUCKET DST_BUCKET`

Copy an existing bucket to a new bucket.
The destination bucket must exist when:

- the destination bucket is a cloud one
- a multi-object operation is requested (either flag `--list` or `--template` is set)

### Options
| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--dry-run` | `bool` | Don't actually copy bucket, only include stats what would happen | `false` |
| `--prefix` | `string` | Prefix added to every new object's name | `""` |
| `--wait` | `bool` | Wait until copying of a bucket is finished | `false` |
| `--list` | `string` | Comma-separated list of objects to copy | `""` |
| `--template` | `string` | Copy only objects which names match the pattern | `""` |

Flags `--list` and `--template` are mutually exclusive.

### Examples

#### Copy AIS bucket

Copy AIS bucket `src_bucket` to AIS bucket `dst_bucket`.

```console
$ ais bucket cp ais://src_bucket ais://dst_bucket
Copying bucket "ais://bucket_name" to "ais://dst_bucket" in progress.
To check the status, run: ais show job xaction copy-bck ais://dst_bucket
```

#### Copy AIS bucket and wait until finish

The same as above, but wait until copying is finished.

```console
$ ais bucket cp ais://src_bucket ais://dst_bucket --wait
```

#### Copy cloud bucket to another cloud bucket

Copy AWS bucket `src_bucket` to AWS bucket `dst_bucket`.

```console
# Make sure that both buckets exist.
$ ais bucket ls aws://
AWS Buckets (2)
  aws://src_bucket
  aws://dst_bucket
$ ais bucket cp aws://src_bucket aws://dst_bucket
Copying bucket "aws://src_bucket" to "aws://dst_bucket" in progress.
To check the status, run: ais show job xaction copy-bck aws://dst_bucket
```

#### Copy only selected objects

Copy objects `obj1.tar` and `obj1.info` from bucket `ais://bck1` to `ais://bck2`, and wait until the operation finishes.

```console
$ ais bucket cp ais://bck1 ais://bck2 --list obj1.tar,obj1.info --wait
copying objects operation ("ais://bck1" => "ais://bck2") is in progress...
copying objects operation succeeded.
```

Copy object with pattern matching: copy `obj2`, `obj3`, and `obj4` from `ais://bck1` to `ais://bck2`.
Do not wait for the operation is done.

```console
$ ais bucket cp ais://bck1 ais://bck2 --template "obj{2..4}"
copying objects operation ("ais://bck1" => "ais://bck2") is in progress...
To check the status, run: ais show job xaction copy-bck ais://bck2
```

## Show bucket summary

`ais bucket summary [BUCKET]`

Show summary information on a per bucket basis. If bucket is omitted, the command *applies* to all [AIS buckets](/docs/bucket.md#ais-bucket).

The output includes the total number of objects in a bucket, the bucket's size (bytes, megabytes, etc.), and the percentage of the total capacity used by the bucket.

A few additional words must be said about `--validate`. The option is provided to run integrity checks, namely: locations of objects, replicas, and EC slices in the bucket, the number of replicas (and whether this number agrees with the bucket configuration), and more.

> Location of each stored object must at any point in time correspond to the current cluster map and, within each storage target, to the target's [mountpaths](/docs/overview.md#terminology). A failure to abide by location rules is called *misplacement*; misplaced objects - if any - must be migrated to their proper locations via automated processes called `global rebalance` and `resilver`:

* [global rebalance and reslver](/docs/rebalance.md)
* [resilvering selected targets: advanced usage](/docs/resourcesvanced.md)

### Bucket summary: important notes

1. `--validate` may take considerable time to execute (depending, of course, on sizes of the datasets in question and the capabilities of the underlying hardware);
2. non-zero *misplaced* objects in the (validated) output is a direct indication that the cluster requires rebalancing and/or resilvering;
3. `--fast=false` is another command line option that may also significantly increase execution time;
4. by default, `--fast` is set to `true`, which also means that bucket summary executes a *faster* logic (that may have a certain minor speed/accuracy trade-off);
5. to obtain the most precise results, run the command with `--fast=false` - and prepare to wait.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--fast` | `bool` | Use faster logic to count objects and report disk usage (default: true) | `true` |
| `--validate` | `bool` | Check buckets for errors: misplaced objects, insufficient number of replicas, and more | `false` |
| `--verbose` | `bool` | Verbose output | `false` |
| `--cached` | `bool` | For buckets that have remote [backends](/docs/providers.md), list only the objects that are stored in the cluster | `false` |

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

`ais job start mirror BUCKET --copies <value>`

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
$ ais bucket create ais://bck --bucket-props "ec.enabled=true ec.data_slices=6 ec.parity_slices=4"
Create bucket "ais://bck" failed: EC config (6 data, 4 parity) slices requires at least 11 targets (have 6)
$
$ ais bucket create ais://bck --bucket-props "ec.enabled=true ec.data_slices=6 ec.parity_slices=4" --force
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
