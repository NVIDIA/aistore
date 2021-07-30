---
layout: post
title: HOWTO_BENCHMARK
permalink: docs/howto_benchmark
redirect_from:
 - docs/howto_benchmark.md/
---

## How to benchmark with AIStore

The tool we have developed is called AIS Loader (`aisloader`) - a load generator to benchmark and stress-test AIStore, or any S3-compatible backend. AIS Loader generates arbitrary read/write workloads including those (synthetic) ones that mimic training and inference workloads. The latter allows to run benchmarks in isolation and avoid compute-side bottlenecks, if any.

For usage, run: `aisloader`, or `aisloader usage`, or `aisloader --help`.

To get started, go to the `aistore` root directory and run:

```console
$ make aisloader
$ $GOPATH/bin/aisloader
```

Following in this document are a few *easy* usage examples and dry-run instructions. For detailed description and more examples, please see [aisloader readme](/bench/aisloader/README.md).

### Basic Examples

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

1. Destroy existing ais bucket - the first example. Delete all objects in a given Cloud-based bucket - the second example:

```console
$ aisloader -bucket=nvais -duration 0s -totalputsize=0
$ aisloader -bucket=aws://nvais -cleanup=true -duration 0s -totalputsize=0
```

2. Time-based 100% PUT into ais bucket. Upon exit the bucket is emptied (by default):

```console
$ aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais
```

3. 100% GET from an ais bucket:

```console
$ aisloader -bucket=nvais -duration 5s -numworkers=3 -pctput=0 -provider=ais
```

4. Mixed 30%/70% PUT and GET of variable-size objects to/from a Cloud bucket. PUT will generate random object names and is limited by the 10GB total size. Cleanup is not disabled, which means that upon completion all generated objects will be deleted:

```console
$ aisloader -bucket=nvaws -duration 0s -numworkers=3 -minsize=1024 -maxsize=1MB -pctput=30 -provider=cloud -totalputsize=10G
```

5. PUT 2000 objects named as `aisloader/hex({0..2000}{loaderid})`:

```console
$ aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -objNamePrefix="aisloader"
```

7. 30% PUT of random TAR files and 70% GET of transformed TARs to TFRecords with tar2tf transformation
```console
$ aisloader -bucket=nvaws -duration=5m -numworkers=3 -pctput=30 -readertype=tar -transformation=tar2tf
```

### AIS Loader full documentation

Full aisloader documentation, including more examples and full options list, is available [here](/bench/aisloader/README.md).


## Dry-Run Performance Tests

AIStore supports a "dry" deployment: `AIS_NO_DISK_IO`.

Example of deploying a cluster with disk IO disabled and object size 256KB:

```console
$ AIS_NO_DISK_IO=true AIS_DRY_OBJ_SIZE=256k make deploy
```

**Note:** These are passed in either as environment variables or command line arguments when deploying the AIStore clusters.

| CLI Argument | Environment Variable | Default Value | Behavior |
| ------------ | ------ | ------ | ------------- |
| nodiskio | `AIS_NO_DISK_IO` | `false` | If `true` - disables disk IO. For GET requests, a storage target does not read anything from disks - no file stat, file open etc - and returns an in-memory object with predefined size (see `AIS_DRY_OBJ_SIZE` variable). For PUT requests, it reads the request's body to `/dev/null`. <br> Valid values are `true` or `1`, and `false` or `0`. |
| dryobjsize | `AIS_DRY_OBJ_SIZE` | `8m` | A size of an object when a source is a 'fake' one: disk IO disabled for GET requests, and network IO disabled for PUT requests. The size is in bytes but suffixes can be used. The following suffixes are supported: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. <br> Default value is '8m' - the size of an object is 8 megabytes |
