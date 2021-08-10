---
layout: post
title: HOW TO BENCHMARK
permalink: /docs/howto-benchmark
redirect_from:
 - /howto_benchmark.md/
 - /docs/howto_benchmark.md/
---

## How to benchmark with AIStore

The tool we have developed is called AIS Loader (`aisloader`) - a load generator to benchmark and stress-test AIStore, or any S3-compatible backend. AIS Loader generates arbitrary read/write workloads including those (synthetic) ones that mimic training and inference workloads. The latter allows to run benchmarks in isolation and avoid compute-side bottlenecks, if any.

For usage, run: `aisloader`, or `aisloader usage`, or `aisloader --help`.

To get started, go to the `aistore` root directory and run:

```console
$ make aisloader
$ $GOPATH/bin/aisloader
```

Following in this document are a few *easy* usage examples and dry-run instructions. For detailed description and more examples, please see [aisloader readme](/docs/aisloader.md).

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

6. PUT 30% of random TAR files and 70% GET of transformed TARs to TFRecords with tar2tf transformation.

    ```console
    $ aisloader -bucket=nvaws -duration=5m -numworkers=3 -pctput=30 -readertype=tar -transformation=tar2tf
    ```

### AIS Loader full documentation

Full aisloader documentation, including more examples and full options list, is available [here](/docs/aisloader.md).
