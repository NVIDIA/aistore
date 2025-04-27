---
layout: post
title: BUCKET
permalink: /docs/cli/bucket
redirect_from:
 - /cli/bucket.md/
 - /docs/cli/bucket.md/
---

It is easy to see all CLI operations on *buckets*:

```console
$ ais bucket <TAB-TAB>

ls        summary   lru       evict     show      create    cp        mv        rm        props
```

For convenience, a few of the most popular verbs are also aliased:

```console
$ ais alias | grep bucket
cp              bucket cp
create          bucket create
evict           bucket evict
ls              bucket ls
rmb             bucket rm
```

> For types of supported buckets (AIS, Cloud, backend, etc.) and many more examples, see [in-depth overview](/docs/bucket.md).

## Table of Contents
- [Create bucket](#create-bucket)
- [Delete bucket](#delete-bucket)
- [List buckets](#list-buckets)
- [List objects](#list-objects)
- [Evict remote bucket](#evict-remote-bucket)
- [Move or Rename a bucket](#move-or-rename-a-bucket)
- [Copy (list, range, and/or prefix) selected objects or entire (in-cluster or remote) buckets](#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)
- [Example copying buckets and multi-objects with simultaneous synchronization](#example-copying-buckets-and-multi-objects-with-simultaneous-synchronization)
- [Show bucket summary](#show-bucket-summary)
- [Start N-way Mirroring](#start-n-way-mirroring)
- [Start Erasure Coding](#start-erasure-coding)
- [Show bucket properties](#show-bucket-properties)
- [Set bucket properties](#set-bucket-properties)
- [Archive multiple objects](#archive-multiple-objects)
- [Show and set AWS-specific properties](#show-and-set-aws-specific-properties)
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

`ais ls PROVIDER:[//BUCKET_NAME]` [command options]

**Notice** the optional `[//BUCKET_NAME]`. When there's no bucket, `ais ls` will list **buckets**. Otherwise, it'll list **objects**.

## Usage

```console
$ ais ls --help

NAME:
   ais ls - (alias for "bucket ls") list buckets, objects in buckets, and files in (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted objects,
   e.g.:
     * ais ls                                              - list all buckets in a cluster (all providers);
     * ais ls ais://abc -props name,size,copies,location   - list all objects from a given bucket, include only the (4) specified properties;
     * ais ls ais://abc -props all                         - same as above but include all properties;
     * ais ls ais://abc --page-size 20 --refresh 3s        - list a very large bucket (20 items in each page), report progress every 3s;
     * ais ls ais                                          - list all ais buckets;
     * ais ls s3                                           - list all s3 buckets that are present in the cluster;
     * ais ls s3 --all                                     - list all s3 buckets, both present and remote;
   with template, regex, and/or prefix:
     * ais ls gs: --regex "^abc" --all                       - list all accessible GCP buckets with names starting with "abc";
     * ais ls ais://abc --regex ".md" --props size,checksum  - list *.md objects with their respective sizes and checksums;
     * ais ls gs://abc --template images/                    - list all objects from the virtual subdirectory called "images";
     * ais ls gs://abc --prefix images/                      - same as above (for more examples, see '--template' below);
     * ais ls gs://abc/images/                               - same as above.
   and summary (stats):
     * ais ls s3 --summary                  - for each s3 bucket in the cluster: print object numbers and total size(s);
     * ais ls s3 --summary --all            - generate summary report for all s3 buckets; include remote objects and buckets that are _not present_;
     * ais ls s3 --summary --all --dont-add - same as above but without adding _non-present_ remote buckets to cluster's BMD.
```

## Assorted options

The options are numerous. Here's a non-exhaustive list (for the most recent update, run `ais ls --help`)

```console
OPTIONS:
   --all                depending on the context:
                        - all objects in a given bucket, including misplaced and copies, or
                        - all buckets, including accessible (visible) remote buckets that are _not present_ in the cluster
   --cached             list only those objects from a remote bucket that are present ("cached")
   --name-only          faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)
   --props value        comma-separated list of object properties including name, size, version, copies and more; e.g.:
                        --props all
                        --props name,size,cached
                        --props "ec, copies, custom, location"
   --regex value        regular expression; use it to match either bucket names or objects in a given bucket, e.g.:
                        ais ls --regex "(m|n)"         - match buckets such as ais://nnn, s3://mmm, etc.;
                        ais ls ais://nnn --regex "^A"  - match object names starting with letter A
   --summary            show object numbers, bucket sizes, and used capacity; applies _only_ to buckets and objects that are _present_ in the cluster
   --units value        show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
                        raw - do not convert to (or from) human-readable format
   --no-headers, -H     display tables without headers
   --no-footers         display tables without footers
```

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

As a rule of thumb, when a (logical) `#namespace` in the bucket's name is omitted we use the global namespace that always exists.

## List objects

`ais ls` is one of those commands that only keeps growing, in terms of supported options and capabilities.

The command:

`ais ls PROVIDER:[//BUCKET_NAME]` [command options]

can conveniently list buckets (with or without "summarizing" object counts and sizes) and objects.

Notice the optional `[//BUCKET_NAME]`. When there's no bucket, `ais ls` will list **buckets**. Otherwise, it'll list **objects**.

The command's inline help is also quite extensive, with (inline) examples followed by numerous supported options:

```console
$ ais ls --help
NAME:
   ais ls - (alias for "bucket ls") List buckets, objects in buckets, and files in (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted objects,
   e.g.:
     * ais ls                                              - list all buckets in a cluster (all providers);
     * ais ls ais://abc -props name,size,copies,location   - list all objects from a given bucket, include only the (4) specified properties;
     * ais ls ais://abc -props all                         - same as above but include all properties;
     * ais ls ais://abc --page-size 20 --refresh 3s        - list a very large bucket (20 items in each page), report progress every 3s;
     * ais ls ais://abc --page-size 20 --refresh 3         - same as above;
     * ais ls ais                                          - list all ais buckets;
     * ais ls s3                                           - list all s3 buckets that are present in the cluster;
     * ais ls s3 --all                                     - list all s3 buckets, both in-cluster and remote.
   with template, regex, and/or prefix:
     * ais ls gs: --regex "^abc" --all                        - list all accessible GCP buckets with names starting with "abc";
     * ais ls ais://abc --regex ".md" --props size,checksum   - list *.md objects with their respective sizes and checksums;
     * ais ls gs://abc --template images/                     - list all objects from the virtual subdirectory called "images";
     * ais ls gs://abc --prefix images/                       - same as above (for more examples, see '--template' below);
     * ais ls gs://abc/images/                                - same as above.
   with in-cluster vs remote content comparison (diff):
     * ais ls s3://abc --check-versions           - for each remote object in s3://abc: check whether it has identical in-cluster copy
                                                    and show missing objects;
     * ais ls s3://abc --check-versions --cached  - for each in-cluster object in s3://abc: check whether it has identical remote copy
                                                    and show deleted objects.
   with summary (bucket sizes and numbers of objects):
     * ais ls ais://nnn --summary --prefix=aaa/bbb'   - summarize objects that match a given prefix;
     * ais ls ais://nnn/aaa/bbb --summary'            - same as above;
     * ais ls s3 --summary                            - for each s3 bucket: print number of objects and total size (bytes);
     * ais ls s3 --summary --all                      - generate summary report for all s3 buckets; include remote objects and buckets that are _not present_;
     * ais ls s3 --summary --all --dont-add           - same as above but without adding _non-present_ remote buckets to the cluster's BMD.

USAGE:
   ais ls [BUCKET[/PREFIX]] [PROVIDER] [command options]

OPTIONS:
   --all                  Depending on the context, list:
                          - all buckets, including accessible (visible) remote buckets that are not in-cluster
                          - all objects in a given accessible (visible) bucket, including remote objects and misplaced copies
   --archive              List archived content (see docs/archive.md for details)
   --cached               Only list in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   --count-only           Print only the resulting number of listed objects and elapsed time
   --diff                 Perform a bidirectional diff between in-cluster and remote content, which further entails:
                          - detecting remote version changes (a.k.a. out-of-band updates), and
                          - remotely deleted objects (out-of-band deletions (*));
                            the option requires remote backends supporting some form of versioning (e.g., object version, checksum, and/or ETag);
                          see related:
                               (*) options: --cached; --latest
                               commands:    'ais get --latest'; 'ais cp --sync'; 'ais prefetch --latest'
   --dont-add             List remote bucket without adding it to cluster's metadata - e.g.:
                            - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);
                            - then, if we ask aistore to list remote buckets: `ais ls s3://abc --all'
                              the bucket will be added (in effect, it'll be created);
                            - to prevent this from happening, either use this '--dont-add' flag or run 'ais evict' command later
   --dont-wait            When _summarizing_ buckets do not wait for the respective job to finish -
                          use the job's UUID to query the results interactively
   --inv-id value         Bucket inventory ID (optional; by default, we use bucket name as the bucket's inventory ID)
   --inv-name value       Bucket inventory name (optional; system default name is '.inventory')
   --inventory            List objects using _bucket inventory_ (docs/s3inventory.md); requires s3:// backend; will provide significant performance
                          boost when used with very large s3 buckets; e.g. usage:
                            1) 'ais ls s3://abc --inventory'
                            2) 'ais ls s3://abc --inventory --paged --prefix=subdir/'
                          (see also: docs/s3inventory.md)
   --limit value          The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                          e.g.:
                          - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                          - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                          - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/-- (default: 0)
   --max-pages value      Maximum number of pages to display (see also '--page-size' and '--limit')
                          e.g.: 'ais ls az://abc --paged --page-size 123 --max-pages 7 (default: 0)
   --name-only            Faster request to retrieve only the names of objects (if defined, '--props' flag will be ignored)
   --no-dirs              Do not return virtual subdirectories (applies to remote buckets only)
   --no-footers, -F       Display tables without footers
   --no-headers, -H       Display tables without headers
   --non-recursive, --nr  Non-recursive operation, e.g.:
                          - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                          - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                          - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                          - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --page-size value      Maximum number of object names per page; when the flag is omitted or 0 (zero)
                          the maximum is defined by the corresponding backend; see also '--max-pages' and '--paged' (default: 0)
   --paged                List objects page by page - one page at a time (see also '--page-size' and '--limit')
                          note: recommended for use with very large buckets
   --prefix value         List objects with names starting with the specified prefix, e.g.:
                          '--prefix a/b/c' - list virtual directory a/b/c and/or objects from the virtual directory
                          a/b that have their names (relative to this directory) starting with the letter 'c'
   --props value          Comma-separated list of object properties including name, size, version, copies, and more; e.g.:
                          --props all
                          --props name,size,cached
                          --props "ec, copies, custom, location"
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --regex value          Regular expression; use it to match either bucket names or objects in a given bucket, e.g.:
                          ais ls --regex "(m|n)"         - match buckets such as ais://nnn, s3://mmm, etc.;
                          ais ls ais://nnn --regex "^A"  - match object names starting with letter A
   --show-unmatched       List also objects that were not matched by regex and/or template (range)
   --silent               Server-side flag, an indication for aistore _not_ to log assorted errors (e.g., HEAD(object) failures)
   --skip-lookup          Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                           1) adding remote bucket to aistore without first checking the bucket's accessibility
                              (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                           2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --start-after value    List bucket's content alphabetically starting with the first name _after_ the specified
   --summary              Show object numbers, bucket sizes, and used capacity;
                          note: applies only to buckets and objects that are _present_ in the cluster
   --template value       Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                          (with optional steps and gaps), e.g.:
                          --template "" # (an empty or '*' template matches eveything)
                          --template 'dir/subdir/'
                          --template 'shard-{1000..9999}.tar'
                          --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                          and similarly, when specifying files and directories:
                          --template '/home/dir/subdir/'
                          --template "/abc/prefix-{0010..9999..2}-suffix"
   --units value          Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                          iec - IEC format, e.g.: KiB, MiB, GiB (default)
                          si  - SI (metric) format, e.g.: KB, MB, GB
                          raw - do not convert to (or from) human-readable format
   --help, -h             Show help
```

### Assorted options

| Name | Type | Description | Default |
| --- | --- | --- | --- |
| `--regex` | `string` | regular expression to match and select items in question | `""` |
| `--template` | `string` | template for matching object names, e.g.: 'shard-{900..999}.tar' | `""` |
| `--prefix` | `string` | list objects matching a given prefix | `""` |
| `--page-size` | `int` | maximum number of names per page (0 - the maximum is defined by the corresponding backend) | `0` |
| `--props` | `string` | comma-separated list of object properties including name, size, version, copies, EC data and parity info, custom metadata, location and more; to include all properties, type '--props all' (default: "name,size") | `"name,size"` |
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
| `--skip-lookup` | `bool` | list public-access Cloud buckets that may disallow certain operations (e.g., `HEAD(bucket)`); use this option for performance _or_ to read Cloud buckets that allow _anonymous_ access | `false` |
| `--archive` | `bool` | list archived content | `false` |
| `--check-versions` | `bool` | check whether listed remote objects and their in-cluster copies are identical, ie., have the same versions; applies to remote backends that maintain at least some form of versioning information (e.g., version, checksum, ETag) | `false` |
| `--summary` | `bool` | show bucket sizes and used capacity; by default, applies only to the buckets that are _present_ in the cluster (use '--all' option to override) | `false` |
| `--bytes` | `bool` | show sizes in bytes (ie., do not convert to KiB, MiB, GiB, etc.) | `false` |
| `--name-only` | `bool` | fast request to retrieve only the names of objects in the bucket; if defined, all comma-separated fields in the `--props` flag will be ignored with only two exceptions: `name` and `status` | `false` |

### Footer Information:

When listing objects, a footer will be displayed showing:
- Total number of objects listed
- For remote buckets with `--cached` option: number of objects present in-cluster
- For `--paged` option: current page number
- For `--count-only` option: time elapsed to fetch the list

Examples of footer variations:
* `Listed 12345 names`
* `Listed 12345 names (in-cluster: 456)`
* `Page 123: 1000 names (in-cluster: none)`

### Examples

#### List AIS and Cloud buckets with all defaults

**1**. List objects in the AIS bucket `bucket_name`.

```console
$ ais ls ais://bucket_name
NAME		SIZE
shard-0.tar	16.00KiB
shard-1.tar	16.00KiB
...
```

**2**. List objects in the remote bucket `bucket_name`.

```console
ais ls aws://bucket_name
NAME		SIZE
shard-0.tar	16.00KiB
shard-1.tar	16.00KiB
...
```

**3**. List objects from a remote AIS cluster with a namespace:
```
$ ais ls ais://@Bghort1l#ml/bucket_name
NAME                SIZE        VERSION
shard-0.tar         16.00KiB    1
shard-1.tar         16.00KiB    1
...
```

**4**. List objects with paged output (showing page numbers):
```
$ ais ls ais://bucket_name --paged --limit 100
[... object listing ...]

Page 1: 100 names
```

**5**. List cached objects from a remote bucket:
```
$ ais ls s3://bucket_name --cached
[... listing of only in-cluster objects ...]

Listed 456789 names
```

**6**. Count objects in a bucket:
```
$ ais ls s3://bucket_name/aprefix --count-only
Listed 28,230 names in 5.62s
```

**7**. Count objects with paged output:
```
$ ais ls s3://bucket_name/bprefix --count-only --paged

Page 1: 1,000 names in 772ms
Page 2: 1,000 names in 180ms
Page 3: 1,000 names in 265ms
...
Page 29: 230 names in 130ms
```

### Notes:

- When using `--paged` with remote buckets, the footer will show both page number and in-cluster object count when applicable
- The `--diff` option requires remote backends supporting some form of versioning (e.g., object version, checksum, and/or ETag)
- For more information on working with archived content, see docs/archive.md
- To fully synchronize in-cluster content with remote backend, see documentation on [out-of-band updates](/docs/out_of_band.md)

#### Include all properties

```console
# ais ls gs://webdataset-abc --skip-lookup --props all
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

#### Bucket inventory

Here's a quick 4-steps sequence to demonstrate the functionality:

**1**. In the beginning, the bucket is accessible (notice `--all`) and empty, as far as its in-cluster content
```console
$ ais ls s3://abc --cached --all
NAME                     SIZE
```

**2**. The first (remote) list-objects will have the side-effect of loading remote inventory
```console
$ ais ls s3://abc --inventory --count-only
Note: listing remote objects in s3://abc may take a while
(Tip: use '--cached' to speed up and/or '--paged' to show pages)

Listed 2,319,231 names in 23.91s
```

**3**. The second and later list-objects will run much faster
```console
$ ais ls s3://abc --inventory --count-only
Listed 2,319,231 names in 4.18s
```

**4**. Finally, observe that at in-cluster content now includes the inventory (.csv) itself
```console
$ ais ls s3://abc --cached
NAME                     SIZE
.inventory/ais-vm.csv    143.61MiB
```

#### List archived contect

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
$ ais ls gs://webdataset-abc --skip-lookup
NAME                             SIZE
coco-train2014-seg-000000.tar    958.48MiB
coco-train2014-seg-000001.tar    958.47MiB
coco-train2014-seg-000002.tar    958.47MiB
coco-train2014-seg-000003.tar    958.22MiB
coco-train2014-seg-000004.tar    958.56MiB
coco-train2014-seg-000005.tar    958.19MiB
...
```

#### Use '--prefix' that crosses shard boundary

For starters, we archive all aistore docs:

```console
$ ais put docs ais://A.tar --archive -r
```
To list a certain virtual subdirectory _inside_ this newly created shard:

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

## Evict remote bucket

Evict a [remote bucket](/docs/bucket.md#remote-bucket).

Read more about this feature [here](/docs/bucket.md#evict-remote-bucket).

Some of the supported functionality can be quickly demonstrated with the following examples:

* [CLI: Three Ways to Evict Remote Bucket](/docs/cli/evicting_buckets_andor_data.md)

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

## Copy (list, range, and/or prefix) selected objects or entire (in-cluster or remote) buckets

`ais cp SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] DST_BUCKET` [command options]

```console
$ ais cp --help

NAME:
   ais cp - (alias for "bucket cp") Copy entire bucket or selected objects (to select, use '--list', '--template', or '--prefix'),
     e.g.:
     - 'ais cp gs://webdaset-coco ais://dst'                                    - copy entire Cloud bucket;
     - 'ais cp s3://abc ais://nnn --all'                                        - copy Cloud bucket that may _not_ be present in cluster (and create destination if doesn't exist);
     - 'ais cp s3://abc ais://nnn --all --num-workers 16'                       - same as above employing 16 concurrent workers;
     - 'ais cp s3://abc ais://nnn --all --num-workers 16 --prefix dir/subdir/'  - same as above, but limit copying to a given virtual subdirectory;
     - 'ais cp s3://abc gs://xyz --all'                                         - copy Cloud bucket to another Cloud.
     similar to prefetch:
     - 'ais cp s3://data s3://data --all'  - copy remote source (and create namesake destination in-cluster bucket if doesn't exist).
     synchronize with out-of-band updates:
     - 'ais cp s3://abc ais://nnn --latest'  - copy Cloud bucket; make sure that already present in-cluster copies are updated to the latest versions;
     - 'ais cp s3://abc ais://nnn --sync'    - same as above, but in addition delete in-cluster copies that do not exist (any longer) in the remote source.
     with template, prefix, and progress:
     - 'ais cp s3://abc ais://nnn --prepend backup/'                                              - copy objects into 'backup/' virtual subdirectory in destination bucket;
     - 'ais cp ais://nnn/111 ais://mmm'                                                           - copy all ais://nnn objects that match prefix '111';
     - 'ais cp gs://webdataset-coco ais:/dst --template d-tokens/shard-{000000..000999}.tar.lz4'  - copy up to 1000 objects that share the specified prefix;
     - 'ais cp gs://webdataset-coco ais:/dst --prefix d-tokens/ --progress --all'                 - show progress while copying virtual subdirectory 'd-tokens';
     - 'ais cp gs://webdataset-coco/d-tokens/ ais:/dst --progress --all'                          - same as above.

USAGE:
   ais cp SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] DST_BUCKET [command options]

OPTIONS:
   --all                  Copy all objects from a remote bucket including those that are not present (not cached) in cluster
   --cont-on-err          Keep running archiving xaction (job) in presence of errors in a any given multi-object transaction
   --dry-run              Show total size of new objects without really creating them
   --force, -f            Force execution of the command (caution: advanced usage only)
   --latest               Check in-cluster metadata and, possibly, GET, download, prefetch, or otherwise copy the latest object version
                          from the associated remote bucket;
                          the option provides operation-level control over object versioning (and version synchronization)
                          without the need to change the corresponding bucket configuration: 'versioning.validate_warm_get';
                          see also:
                            - 'ais show bucket BUCKET versioning'
                            - 'ais bucket props set BUCKET versioning'
                            - 'ais ls --check-versions'
                          supported commands include:
                            - 'ais cp', 'ais prefetch', 'ais get'
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
   --num-workers value    Number of concurrent workers (readers); defaults to a number of target mountpaths if omitted or zero;
                          use (-1) to indicate single-threaded serial execution (ie., no workers);
                          any positive value will be adjusted _not_ to exceed the number of target CPUs (default: 0)
   --prefix value         Select virtual directories or objects with names starting with the specified prefix, e.g.:
                          '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                          '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   --prepend value        Prefix to prepend to every object name during operation (copy or transform), e.g.:
                          --prepend=abc   - prefix all object names with "abc"
                          --prepend=abc/  - use "abc" as a virtual directory (note trailing filepath separator)
                                          - during 'copy', this flag applies to copied objects
                                          - during 'transform', this flag applies to transformed objects
   --progress             Show progress bar(s) and progress of execution in real time
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --sync                 Fully synchronize in-cluster content of a given remote bucket with its (Cloud or remote AIS) source;
                          the option is, effectively, a stronger variant of the '--latest' (option):
                          in addition to bringing existing in-cluster objects in-sync with their respective out-of-band updates (if any)
                          it also entails removing in-cluster objects that are no longer present remotely;
                          like '--latest', this option provides operation-level control over synchronization
                          without requiring to change the corresponding bucket configuration: 'versioning.synchronize';
                          see also:
                            - 'ais show bucket BUCKET versioning'
                            - 'ais bucket props set BUCKET versioning'
                            - 'ais ls --check-versions'
   --template value       Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                          (with optional steps and gaps), e.g.:
                          --template "" # (an empty or '*' template matches eveything)
                          --template 'dir/subdir/'
                          --template 'shard-{1000..9999}.tar'
                          --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                          and similarly, when specifying files and directories:
                          --template '/home/dir/subdir/'
                          --template "/abc/prefix-{0010..9999..2}-suffix"
   --timeout value        Maximum time to wait for a job to finish; if omitted: wait forever or until Ctrl-C;
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --wait                 Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --help, -h             Show help
```

Source bucket must exist. When the destination bucket is remote (e.g. in the Cloud) it must also exist and be writeable.

> **NOTE:** there's _no_ requirement that either of the buckets is _present_ in aistore.

> **NOTE:** not to confuse in-cluster _presence_ and existence. Remote object may exist (remotely), etc.

> **NOTE:** to fully synchronize in-cluster content with remote backend, please refer to [out of band updates](/docs/out_of_band.md).

Moreover, when the destination is AIS (`ais://`) or remote AIS (`ais://@remote-alias`) bucket, the existence is optional: the destination will be created on the fly, with bucket properties copied from the source (`SRC_BUCKET`).

>  **NOTE:** similar to delete, evict and prefetch operations, `cp` also supports embedded prefix - see [disambiguating multi-object operation](/docs/cli/object.md#disambiguating-multi-object-operation)

Finally, the option to copy remote bucket onto itself is also supported - syntax-wise. Here's an example that'll shed some light:

```console
## 1. at first, we don't have any gs:// buckets in the cluster

$ ais ls gs
No "gs://" buckets in the cluster. Use '--all' option to list matching remote buckets, if any.

## 2. notwithstanding, we go ahead and start copying gs://coco-dataset

$ ais cp gs://coco-dataset gs://coco-dataset --prefix d-tokens --progress --all
Copied objects:                  282/393 [===========================================>------------------] 72 %
Copied size:    719.48 MiB / 1000.08 MiB [============================================>-----------------] 72 %

## 3. and done: all 393 objects from the remote bucket are now present ("cached") in the cluster

$ ais ls gs://coco-dataset --cached | grep Listed
Listed: 393 names
```

> Incidentally, notice the `--cached` difference:

```console
$ ais ls gs://coco-dataset --cached | grep Listed
Listed: 393 names

## vs _all_ including remote:

$ ais ls gs://coco-dataset | grep Listed
Listed: 2,290 names
```

### Examples

#### Copy _non-existing_ remote bucket to a non-existing in-cluster destination

```console
$ ais ls s3
No "s3://" buckets in the cluster. Use '--all' option to list matching remote buckets, if any.

$ ais cp s3://abc ais://nnn --all
Warning: destination ais://nnn doesn't exist and will be created with configuration copied from the source (s3://abc))
Copying s3://abc => ais://nnn. To monitor the progress, run 'ais show job tco-JcTKbhvFy'
```

#### Copy AIS bucket

Copy AIS bucket `src_bucket` to AIS bucket `dst_bucket`.

```console
$ ais cp ais://src_bucket ais://dst_bucket
Copying bucket "ais://bucket_name" to "ais://dst_bucket" in progress.
To check the status, run: ais show job xaction copy-bck ais://dst_bucket
```

#### Copy AIS bucket and wait until the job finishes

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

### Use (list, range, and/or prefix) options to copy selected objects

**Example 1.** Copy objects `obj1.tar` and `obj1.info` from bucket `ais://bck1` to `ais://bck2`, and wait until the operation finishes

```console
$ ais cp ais://bck1 ais://bck2 --list obj1.tar,obj1.info --wait
copying objects operation ("ais://bck1" => "ais://bck2") is in progress...
copying objects operation succeeded.
```

**Example 2.** Copy objects matching Bash brace-expansion `obj{2..4}, do not wait for the operation is done.

```console
$ ais cp ais://bck1 ais://bck2 --template "obj{2..4}"
copying objects operation ("ais://bck1" => "ais://bck2") is in progress...
To check the status, run: ais show job xaction copy-bck ais://bck2
```

**Example 3.** Use `--sync` option to copy remote virtual subdirectory

```console
$ ais cp gs://coco-dataset --sync --prefix d-tokens
Copying objects gs://coco-dataset. To monitor the progress, run 'ais show job tco-kJPUtYJld'
```

In the example, `--sync` synchronizes destination bucket with its remote (e.g., Cloud) source.

In particular, the option will make sure that aistore has the **latest** versions of remote objects _and_ may also entail **removing** of the objects that no longer exist remotely

### See also

* [Out of band updates](/docs/out_of_band.md)

## Example copying buckets and multi-objects with simultaneous synchronization

There's a [script](https://github.com/NVIDIA/aistore/blob/main/ais/test/scripts/cp-sync-remais-out-of-band.sh) that we use for testing. When run, it produces the following output:

```console
$ ./ais/test/scripts/cp-sync-remais-out-of-band.sh --bucket gs://abc

 1. generate and write 500 random shards => gs://abc
 2. copy gs://abc => ais://dst-9408
 3. remove 10 shards from the source
 4. copy gs://abc => ais://dst-9408 w/ synchronization ('--sync' option)
 5. remove another 10 shards
 6. copy multiple objects using bash-expansion defined range and '--sync'
 #
 # out of band DELETE using remote AIS (remais)
 #
 7. use remote AIS cluster ("remais") to out-of-band remove 10 shards from the source
 8. copy gs://abc => ais://dst-9408 w/ --sync
 9. when copying, we always synchronize content of the in-cluster source as well
10. use remais to out-of-band remove 10 more shards from gs://abc source
11. copy a range of shards from gs://abc to ais://dst-9408, and compare
12. and again: when copying, we always synchronize content of the in-cluster source as well
 #
 # out of band ADD using remote AIS (remais)
 #
13. use remais to out-of-band add (i.e., PUT) 17 new shards
14. copy a range of shards from gs://abc to ais://dst-9408, and check whether the destination has new shards
15. compare the contents but NOTE: as of v3.22, this part requires multi-object copy (using '--list' or '--template')
```

The [script](https://github.com/NVIDIA/aistore/blob/main/ais/test/scripts/cp-sync-remais-out-of-band.sh) executes a sequence of steps (above).

Notice a certain limitation (that also shows up as the last step #15):

* As of the version 3.22, aistore `cp` commands will always synchronize _deleted_ and _updated_ remote content.

* However, to see an out-of-band added content, you currently need to run [multi-object copy](#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets), with multiple source objects specified using `--list` or `--template`.

### See also

* `ais cp --help` for the most recently updated options
* to fully synchronize in-cluster content with remote backend, please refer to [out of band updates](/docs/out_of_band.md)

## Show bucket summary

`ais storage summary PROVIDER:[//BUCKET_NAME] - show bucket sizes and the respective percentages of used capacity on a per-bucket basis [command options]

`ais bucket summary` - same as above.

### Options

```console
$ ais storage summary --help

NAME:
   ais storage summary - Show bucket sizes and %% of used capacity on a per-bucket basis

USAGE:
   ais storage summary [BUCKET[/PREFIX]] [PROVIDER] [command options]

OPTIONS:
   --cached          Only list in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   --count value     Used together with '--refresh' to limit the number of generated reports, e.g.:
                      '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --dont-wait       When _summarizing_ buckets do not wait for the respective job to finish -
                     use the job's UUID to query the results interactively
   --no-headers, -H  Display tables without headers
   --prefix value    For each bucket, select only those objects (names) that start with the specified prefix, e.g.:
                     '--prefix a/b/c' - sum up sizes of the virtual directory a/b/c and objects from the virtual directory
                     a/b that have names (relative to this directory) starting with the letter c
   --refresh value   Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --units value     Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --verbose, -v     Verbose output
   --help, -h        Show help
```

If `BUCKET` is omitted, the command *applies* to all [AIS buckets](/docs/bucket.md#ais-bucket).

The output includes the total number of objects in a bucket, the bucket's size (bytes, megabytes, etc.), and the percentage of the total capacity used by the bucket.

A few additional words must be said about `--validate`. The option is provided to run integrity checks, namely: locations of objects, replicas, and EC slices in the bucket, the number of replicas (and whether this number agrees with the bucket configuration), and more.

> Location of each stored object must at any point in time correspond to the current cluster map and, within each storage target, to the target's [mountpaths](/docs/overview.md#terminology). A failure to abide by location rules is called *misplacement*; misplaced objects - if any - must be migrated to their proper locations via automated processes called `global rebalance` and `resilver`:

* [global rebalance and reslver](/docs/rebalance.md)
* [resilvering selected targets: advanced usage](/docs/resourcesvanced.md)

### Notes

> `--validate` may take considerable time to execute (depending, of course, on sizes of the datasets in question and the capabilities of the underlying hardware);
> non-zero *misplaced* objects in the (validated) output is a direct indication that the cluster requires rebalancing and/or resilvering;
> an alternative way to execute _validation_ is to run `ais strorage validate` or (simply) `ais scrub`:

```console
$ ais scrub --help

NAME:
   ais scrub - (alias for "storage validate") Check in-cluster content for misplaced objects, objects that have insufficient numbers of copies, zero size, and more
   e.g.:
     * ais storage validate                 - validate all in-cluster buckets;
     * ais scrub                            - same as above;
     * ais storage validate ais             - validate (a.k.a. scrub) all ais:// buckets;
     * ais scrub s3                         - ditto, all s3:// buckets;
     * ais scrub s3 --refresh 10            - same as above while refreshing runtime counter(s) every 10s;
     * ais scrub gs://abc/images/           - validate part of the gcp bucket under 'images/`;
     * ais scrub gs://abc --prefix images/  - same as above.

USAGE:
   ais scrub [BUCKET[/PREFIX]] [PROVIDER] [command options]

OPTIONS:
   --all-columns          Show all columns, including those with only zero values
   --cached               Only visit in-cluster objects, i.e., objects from the respective remote bucket that are present ("cached") in the cluster
   --count value          Used together with '--refresh' to limit the number of generated reports, e.g.:
                           '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --large-size value     Count and report all objects that are larger or equal in size  (e.g.: 4mb, 1MiB, 1048576, 128k; default: 5 GiB)
   --limit value          The maximum number of objects to list, get, or otherwise handle (0 - unlimited; see also '--max-pages'),
                          e.g.:
                          - 'ais ls gs://abc/dir --limit 1234 --cached --props size,custom,atime'  - list no more than 1234 objects
                          - 'ais get gs://abc /dev/null --prefix dir --limit 1234'                 - get --/--
                          - 'ais scrub gs://abc/dir --limit 1234'                                  - scrub --/-- (default: 0)
   --max-pages value      Maximum number of pages to display (see also '--page-size' and '--limit')
                          e.g.: 'ais ls az://abc --paged --page-size 123 --max-pages 7 (default: 0)
   --no-headers, -H       Display tables without headers
   --non-recursive, --nr  Non-recursive operation, e.g.:
                          - 'ais ls gs://bucket/prefix --nr'   - list objects and/or virtual subdirectories with names starting with the specified prefix;
                          - 'ais ls gs://bucket/prefix/ --nr'  - list contained objects and/or immediately nested virtual subdirectories _without_ recursing into the latter;
                          - 'ais prefetch s3://bck/abcd --nr'  - prefetch a single named object (see 'ais prefetch --help' for details);
                          - 'ais rmo gs://bucket/prefix --nr'  - remove a single object with the specified name (see 'ais rmo --help' for details)
   --page-size value      Maximum number of object names per page; when the flag is omitted or 0
                          the maximum is defined by the corresponding backend; see also '--max-pages' and '--paged' (default: 0)
   --prefix value         For each bucket, select only those objects (names) that start with the specified prefix, e.g.:
                          '--prefix a/b/c' - sum up sizes of the virtual directory a/b/c and objects from the virtual directory
                          a/b that have names (relative to this directory) starting with the letter c
   --refresh value        Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                          valid time units: ns, us (or µs), ms, s (default), m, h
   --small-size value     Count and report all objects that are smaller or equal in size (e.g.: 4, 4b, 1k, 128kib; default: 0)
   --help, -h             Show help
```

For details and additional examples, please see:

* [Validate in-cluster content for misplaced objects and missing copies](/docs/cli/storage.md#validate-in-cluster-content-for-misplaced-objects-and-missing-copies)

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
# 3.  "summarize" all s3:// buckets; count both "cached" and remote objects:
$ ais bucket summary s3: --all
```

```console
# 4. same as above with progress updates every 3 seconds:
$ ais bucket summary s3: --all --refresh 3
```

```console
# 4. "summarize" a given gs:// bucket; start the job and exit without waiting for it to finish
# (see prompt below):
$ ais bucket summary gs://abc --all --dont-wait

Job summary[wl-s5lIWA] has started. To monitor, run 'ais storage summary gs://abc wl-s5lIWA --dont-wait' or 'ais show job wl-s5lIWA;
see '--help' for details'
```

## Start N-way Mirroring

`ais start mirror BUCKET --copies <value>`

Start an extended action to bring a given bucket to a certain redundancy level (`value` copies). Read more about this feature [here](/docs/storage_svcs.md#n-way-mirror).

### Options

```console
$ ais start mirror --help

NAME:
   ais start mirror - Configure (or unconfigure) bucket as n-way mirror, and run the corresponding batch job, e.g.:
     - 'ais start mirror ais://m --copies 3'  - configure ais://m as a 3-way mirror;
     - 'ais start mirror ais://m --copies 1'  - configure ais://m for no redundancy (no extra copies).
   (see also: 'ais start ec-encode')

USAGE:
   ais start mirror BUCKET [command options]

OPTIONS:
   --copies value       Number of object replicas (default: 1)
   --non-verbose, --nv  Non-verbose (quiet) output, minimized reporting, fewer warnings
   --help, -h           Show help
```

## Start Erasure Coding

`ais start ec-encode BUCKET --data-slices <value> --parity-slices <value>`

Start an extended action that encodes and recovers all objects and slices in a given bucket.
The action enables erasure coding if it is disabled, and runs the encoding for all objects in the bucket in the background.
If erasure coding for the bucket was enabled beforehand, the extended action recovers missing objects and slices if possible.

In case of running the extended action for a bucket that has already erasure coding enabled, you must pass the correct number of parity and data slices in the command-line.
Run `ais bucket props show <bucket-name> ec` to get the current erasure coding settings.
Read more about this feature [here](/docs/storage_svcs.md#erasure-coding).

### Options

```console
$ ais start ec-encode --help

NAME:
   ais start ec-encode - Erasure code entire bucket, e.g.:
     - 'ais start ec-encode ais://nnn -d 8 -p 2'                          - erasure-code ais://nnn for 8 data and 2 parity slices;
     - 'ais start ec-encode ais://nnn --data-slices 8 --parity-slices 2'  - same as above;
     - 'ais start ec-encode ais://nnn --recover'                          - check and make sure that every ais://nnn object is properly erasure-coded.
   see also: 'ais start mirror'

USAGE:
   ais start ec-encode BUCKET [command options]

OPTIONS:
   --data-slices value, -d value    Number of data slices (default: 2)
   --non-verbose, --nv              Non-verbose (quiet) output, minimized reporting, fewer warnings
   --parity-slices value, -p value  Number of parity slices (default: 2)
   --recover                        Check and make sure that each and every object is properly erasure coded
   --help, -h                       Show help
```

All options are required and must be greater than `0`.

## Show bucket properties

Overall, the topic called "bucket properties" is rather involved and includes sub-topics "bucket property inhertance" and "cluster-wide global defaults". For background, please first see:

* [Default Bucket Properties](/docs/bucket.md#default-bucket-properties)
* [Inherited Bucket Properties and LRU](/docs/bucket.md#inherited-bucket-properties-and-lru)
* [Backend Provider](/docs/bucket.md#backend-provider)
* [Global cluster-wide configuration](/docs/configuration.md#cluster-and-node-configuration).

Now, as far as CLI, run the following to list [properties](/docs/bucket.md#properties-and-options) of the specified bucket.
By default, a certain compact form of bucket props sections is presented.

`ais bucket props show BUCKET [PROP_PREFIX] [command options]`

When `PROP_PREFIX` is set, only props that start with `PROP_PREFIX` will be displayed.
Useful `PROP_PREFIX` are: `access, checksum, ec, lru, mirror, provider, versioning`.

> `ais bucket show` is an alias for `ais show bucket` - both can be used interchangeably.

### Options

```console
$ ais bucket props show --help

NAME:
   ais bucket props show - Show bucket properties

USAGE:
   ais bucket props show BUCKET [PROP_PREFIX] [command options]

OPTIONS:
   --add             Add remote bucket to cluster's metadata
                       - let's say, s3://abc is accessible but not present in the cluster (e.g., 'ais ls' returns error);
                       - most of the time, there's no need to worry about it as aistore handles presence/non-presence
                         transparently behind the scenes;
                       - but if you do want to (explicltly) add the bucket, you could also use '--add' option
   --compact, -c     Display properties grouped in human-readable mode
   --json, -j        JSON input/output
   --no-headers, -H  Display tables without headers
   --help, -h        Show help
```

### Examples

#### Show bucket props with provided section

Show only `lru` section of bucket props for `bucket_name` bucket.

```console
$ ais bucket props show s3://bucket-name --compact
PROPERTY	 VALUE
access		 GET,HEAD-OBJECT,PUT,APPEND,DELETE-OBJECT,MOVE-OBJECT,PROMOTE,UPDATE-OBJECT,HEAD-BUCKET,LIST-OBJECTS,PATCH,SET-BUCKET-ACL,LIST-BUCKETS,SHOW-CLUSTER,CREATE-BUCKET,DESTROY-BUCKET,MOVE-BUCKET,ADMIN
checksum	 Type: xxhash | Validate: Nothing
created		 2024-01-31T15:42:59-08:00
ec		 Disabled
lru		 lru.dont_evict_time=2h0m, lru.capacity_upd_time=10m
mirror		 Disabled
present		 yes
provider	 aws
versioning	 Disabled

$ ais bucket props show s3://bucket_name lru --compact
PROPERTY	 VALUE
lru		 lru.dont_evict_time=2h0m, lru.capacity_upd_time=10m

$ ais bucket props show s3://ais-abhishek lru
PROPERTY		 VALUE
lru.capacity_upd_time	 10m
lru.dont_evict_time	 2h0m
lru.enabled		 true
```

## Set bucket properties

`ais bucket props set [OPTIONS] BUCKET JSON_SPECIFICATION|KEY=VALUE [KEY=VALUE...]`

Set bucket properties.
For the available options, see [bucket-properties](/docs/bucket.md#bucket-properties).

If JSON_SPECIFICATION is used, **all** properties of the bucket are set based on the values in the JSON object.

### Options

```console
$ ais bucket props set --help

NAME:
   ais bucket props set - Update bucket properties; the command accepts both JSON-formatted input and plain Name=Value pairs, e.g.:
     * ais bucket props set ais://nnn backend_bck=s3://mmm
     * ais bucket props set ais://nnn backend_bck=none
     * ais bucket props set gs://vvv versioning.validate_warm_get=false versioning.synchronize=true
     * ais bucket props set gs://vvv mirror.enabled=true mirror.copies=4 checksum.type=md5
     * ais bucket props set s3://mmm ec.enabled true ec.data_slices 6 ec.parity_slices 4 --force
     References:
     * for details and many more examples, see docs/cli/bucket.md
     * to show bucket properties (names and current values), use 'ais bucket show'

USAGE:
   ais bucket props set BUCKET JSON-formatted-KEY-VALUE | KEY=VALUE [KEY=VALUE...] [command options]

OPTIONS:
   --force, -f    Force execution of the command (caution: advanced usage only)
   --skip-lookup  Do not execute HEAD(bucket) request to lookup remote bucket and its properties; possible usage scenarios include:
                   1) adding remote bucket to aistore without first checking the bucket's accessibility
                      (e.g., to configure the bucket's aistore properties with alternative security profile and/or endpoint)
                   2) listing public-access Cloud buckets where certain operations (e.g., 'HEAD(bucket)') may be disallowed
   --help, -h     Show help
```

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
$ ais create ais://bck --props "ec.enabled=true ec.data_slices=6 ec.parity_slices=4"
Create bucket "ais://bck" failed: EC config (6 data, 4 parity) slices requires at least 11 targets (have 6)
$
$ ais create ais://bck --props "ec.enabled=true ec.data_slices=6 ec.parity_slices=4" --force
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
"access" set to: "GET,HEAD-OBJECT,PUT,APPEND,DELETE-OBJECT,MOVE-OBJECT,PROMOTE,UPDATE-OBJECT" (was: "GET,HEAD-OBJECT,PUT,APPEND,DELETE-OBJECT,MOVE-OBJECT,PROMOTE,UPDATE-OBJECT,HEAD-BUCKET,LIST-OBJECTS,PATCH,SET-BUCKET-ACL,LIST-BUCKETS,SHOW-CLUSTER,CREATE-BUCKET,DESTROY-BUCKET,MOVE-BUCKET,ADMIN")
"ec.enabled" set to: "true" (was: "false")
"ec.objsize_limit" set to: "256000" (was: "262144")
"lru.capacity_upd_time" set to: "1m" (was: "10m")
"lru.dont_evict_time" set to: "20m" (was: "1s")
"lru.enabled" set to: "true" (was: "false")
"mirror.enabled" set to: "false" (was: "true")

Bucket props successfully updated.
```

```console
$ ais show bucket ais://bucket_name --compact
PROPERTY	 VALUE
access		 GET,HEAD-OBJECT,PUT,APPEND,DELETE-OBJECT,MOVE-OBJECT,PROMOTE,UPDATE-OBJECT
checksum	 Type: xxhash | Validate: ColdGET
created		 2024-02-02T12:57:17-08:00
ec		 2:2 (250KiB)
lru		 lru.dont_evict_time=20m, lru.capacity_upd_time=1m
mirror		 Disabled
present		 yes
provider	 ais
versioning	 Enabled | Validate on WarmGET: no
```

If not all properties are mentioned in the JSON, the missing ones are set to zero values (empty / `false` / `nil`):

```bash
$ ais bucket props set ais://bucket-name '{
  "mirror": {
    "enabled": true,
    "copies": 2
  },
  "versioning": {
    "enabled": true,
    "validate_warm_get": true
  }
}'
"mirror.enabled" set to: "true" (was: "false")
"versioning.validate_warm_get" set to: "true" (was: "false")

Bucket props successfully updated.

$ ais show bucket ais://bucket-name --compact
PROPERTY	 VALUE
access		 GET,HEAD-OBJECT,PUT,APPEND,DELETE-OBJECT,MOVE-OBJECT,PROMOTE,UPDATE-OBJECT,HEAD-BUCKET,LIST-OBJECTS,PATCH,SET-BUCKET-ACL,LIST-BUCKETS,SHOW-CLUSTER,CREATE-BUCKET,DESTROY-BUCKET,MOVE-BUCKET,ADMIN
checksum	 Type: xxhash | Validate: Nothing
created		 2024-02-02T12:52:30-08:00
ec		     Disabled
lru   		 lru.dont_evict_time=2h0m, lru.capacity_upd_time=10m
mirror		 2 copies
present		 yes
provider	 ais
versioning Enabled | Validate on WarmGET: yes
```

## Archive multiple objects

`ais archive bucket` - Archive selected or matching objects from SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] as (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object (a.k.a. shard).

```console
$ ais archive bucket --help

NAME:
   ais archive bucket - Archive selected or matching objects from SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] as
   (.tar, .tgz or .tar.gz, .zip, .tar.lz4)-formatted object (a.k.a. shard),
   e.g.:
     - 'archive bucket ais://src ais://dst/a.tar.lz4 --template "shard-{001..997}"'
     - 'archive bucket "ais://src/shard-{001..997}" ais://dst/a.tar.lz4'                  - same as above (notice double quotes)
     - 'archive bucket "ais://src/shard-{998..999}" ais://dst/a.tar.lz4 --append-or-put'  - append (ie., archive) 2 more objects

USAGE:
   ais archive bucket SRC_BUCKET[/OBJECT_NAME_or_TEMPLATE] DST_BUCKET/SHARD_NAME [command options]

OPTIONS:
   --append-or-put    Append to an existing destination object ("archive", "shard") iff exists; otherwise PUT a new archive (shard);
                      note that PUT (with subsequent overwrite if the destination exists) is the default behavior when the flag is omitted
   --cont-on-err      Keep running archiving xaction (job) in presence of errors in a any given multi-object transaction
   --dry-run          Preview the results without really running the action
   --include-src-bck  Prefix the names of archived files with the source bucket name
   --list value       Comma-separated list of object or file names, e.g.:
                      --list 'o1,o2,o3'
                      --list "abc/1.tar, abc/1.cls, abc/1.jpeg"
                      or, when listing files and/or directories:
                      --list "/home/docs, /home/abc/1.tar, /home/abc/1.jpeg"
   --prefix value     Select virtual directories or objects with names starting with the specified prefix, e.g.:
                      '--prefix a/b/c'   - matches names 'a/b/c/d', 'a/b/cdef', and similar;
                      '--prefix a/b/c/'  - only matches objects from the virtual directory a/b/c/
   --skip-lookup      Skip checking source and destination buckets' existence (trading off extra lookup for performance)
   --template value   Template to match object or file names; may contain prefix (that could be empty) with zero or more ranges
                      (with optional steps and gaps), e.g.:
                      --template "" # (an empty or '*' template matches eveything)
                      --template 'dir/subdir/'
                      --template 'shard-{1000..9999}.tar'
                      --template "prefix-{0010..0013..2}-gap-{1..2}-suffix"
                      and similarly, when specifying files and directories:
                      --template '/home/dir/subdir/'
                      --template "/abc/prefix-{0010..9999..2}-suffix"
   --wait             Wait for an asynchronous operation to finish (optionally, use '--timeout' to limit the waiting time)
   --help, -h         Show help
```

**See also:**

* [Operations on Lists and Ranges (and entire buckets)](/docs/cli/object.md#operations-on-lists-and-ranges-and-entire-buckets) below.
* [Disambiguating multi-object operation](/docs/cli/object.md#disambiguating-multi-object-operation)

## Show and set AWS-specific properties

AIStore supports AWS-specific configuration on a per s3 bucket basis. Any bucket that is backed up by an AWS S3 bucket (**) can be configured to use alternative:

* named AWS profiles (with alternative credentials and/or region)
* alternative s3 endpoints

For background and usage examples, please see [AWS-specific bucket configuration](aws_profile_endpoint.md).

> (**) Terminology-wise, "s3 bucket" is a shortcut phrase indicating a bucket in an AIS cluster that either (A) has the same name (e.g. `s3://abc`) or (B) a differently named AIS bucket that has `backend_bck` property that specifies the s3 bucket in question.

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
