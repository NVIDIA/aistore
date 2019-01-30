## Table of Contents
- [AIS Load Generator](#ais-load-generator)
   - [Cluster Under Test](#cluster-under-test)
   - [Local vs Cloud-based bucket](local-vs-cloud-based-bucket)
   - [Duration](duration)
   - [Write vs Read](write-vs-read)
   - [Read range](read-range)
   - [Cleanup](cleanup)
   - [Object size](object-size)
   - [Grafana and Graphite](grafana-and-graphite)

## AIS Load Generator

`aisloader` is a command-line load generator [included with AIStore](../bench/aisloader/). For usage, cd to the `aisloader` source directory and run:

```shell
$ go install
$ $GOPATH/bin/aisloader --help
```

To run, the loader requires a **certain minimal subset** of command line options. The list includes:

```shell
-ip=<IPv4 or hostname of the current primary proxy>
-port=<TCP port>
-bucket=<bucket-name>
-local=true | false
-duration=<duration in human-readable format, e.g. 120s, 2h, etc.>
-numworkers=...
```

Rest of this document desctibes these command line options and gives examples.

### Cluster Under Test

By default, `aisloader` sends its requests to the localhost at http://127.0.0.1:8080. To specify the primary proxy URL, use the `ip` and `port` command line options, as follows:

```shell
ip=<IPv4 or hostname of the current primary proxy>
port=<TCP port>
```

For example:

```shell
$ aisloader -ip=example.com -port=8080

Starting GMem2, minfree 3.62GiB, low 7.24GiB, timer 1m0s
GMem2: free memory 7.24GiB > 80% total
Nothing to read, bucket is empty
```

### Local vs Cloud-based bucket

In the example above, the "nothing to read" indicates that `aistore` requires a bucket to operate upon. Use the switch '-local' to select between local bucket (`-local=true`) or Cloud-based one (`-local=false`).

> Terminology: the term *local bucket* simply means that the bucket in question does not cache (or tier) and is not backed by the namesake Cloud bucket. AIStore-own local buckets are totally distributed, content-wise, across the entire AIS cluster. All the [supported storage services](../docs/storage_svcs.md) equally apply to both sorts of buckets.

Note that aisloader **will create a local bucket if it does not exist**.

Further, the name of the bucket is set via the option `-bucket=<bucket name>`.
For instance:

```
$ aisloader -ip=example.com -port=8080 -bucket=abc -local=true
```

### Duration

The loads can run for a given period of time (option `-duration <duration>`) or until the specified amount of data is generated (option `-totalputsize=<total size in KBs>`).

If both options are provided the test finishes on the whatever-comes-first basis.

Example 100% write into the bucket "abc" for 2 hours:

```shell
$ aisloader -bucket=abc -local=true -duration 2h -totalputsize=4000000 -pctput=100
```

The above will run for two hours or until it writes around 4GB data into the bucket, whatever comes first.

### Write vs Read

You can choose a percentage of writing (versus reading) by setting the option `-pctput=<put percentage>`.

Example with a mixed PUT=30% and GET=70% load:

```shell
$ aisloader -bucket=abc -duration 5m -pctput=30
```

Example 100% PUT:

```shell
$ aisloader -bucket=abc -duration 5m -pctput=100
```

The duration in both examples above is set to 5 minutes.

> To test 100% read (`-pctput=0`), make sure to fill the bucket beforehand.

#### Read range

The loader can read the entire object (default) **or** a range of object bytes.

To set the offset and length to read, use option `-readoff=<read offset (in bytes)>` and `readlen=<length to read (in bytes)>`.

For convenience, both options support size suffixes: `k` - for KiB, `m` - for MiB, and `g` - for GiB.

Example that reads a 32MiB segment at 1KB offset from each object stored in the bucket "abc":

```shell
$ aisloader -bucket=abc -duration 5m -cleanup=false -readoff=1024 -readlen=32m
```

The test (above) will run for 5 minutes and will not "cleanup" after itself (next section).

### Cleanup

By default, `aisloder` deletes all the data after completing its run. But what if, for instance, you'd want to test reads (`pctput=0`) after having populated the cluster via 100% PUT.

In this and similar cases, disable automatic cleanup by passing the option `cleanup=false`.

Example:

```shell
$ aisloader -bucket=abc -pctput=100 -totalputsize=16348 -cleanup=false
$ aisloader -bucket=abc -duration 1h -pctput=0 -cleanup=true
```

The first line in this example above fills the bucket "abc" with 16MiB of random data. The second - uses existing data to test read perfomance for 1 hour, and then removes all data.

If you just need to cleanup old data prior to running a test, run the loader with 0 (zero) total put size and zero duration:

```shell
$ aisloader -bucket=<bucket to cleanup> -duration 0s -totalputsize=0
```

### Object size

For the PUT workload the loader generates randomly-filled objects. But what about object sizing?

By default, object sizes are randomly selected as well in the range between 1MiB and 1GiB. To set preferred (or fixed) object size(s), use the options `-minsize=<minimal object size in KiB>` and `-maxsize=<maximum object size in KiB>`

### Setting bucket properties

Before starting a test, it is possible to set `mirror` or `EC` properties on a bucket (for background, please see [storage services](../docs/storage_svcs.md)).

> For background on local mirroring and erasure coding (EC), please see [storage services](../docs/storage_svcs.md).

To achive that, use the option `-bprops`. For example:

```shell
$ aisloader -bucket=abc -pctput=0 -cleanup=false -duration 10s -bprops='{"mirror": {"copies": 2, "mirror_enabled": false, "mirror_util_thresh": 5}, "ec_config": {"enabled": false, "data_slices": 2, "parity_slices": 2}}'
```

The above example shows the values that are globally default as of the AIS version 2.0. You can omit the defaults and specify only those values that you'd want to change. For instance, to enable erasure coding on the bucket "abc":

```shell
$ aisloader -bucket=abc -duration 10s -bprops='{"ec_config": {"enabled": true}}'
```

This example sets the number of data and parity slices to 2 which, in turn, requires the cluster to have at least 5 target nodes: 2 for data slices, 2 for parity slices and one for the original object.

> As of v2.0, once erasure coding is enabled, its properties `data_slices` and `parity_slices` cannot be changed on the fly.

The following sequence populates a bucket configured for both local mirroring and erasure coding, and then reads from it for 1h:

```shell
# Fill bucket
$ aisloader -bucket=abc -cleanup=false -pctput=100 -duration 100m -bprops='{"mirror": {"mirror_enabled": true}, "ec_config": {"enabled": true}}'

# Read
$ aisloader -bucket=abc -cleanup=false -pctput=0 -duration 1h
```

### Grafana and Graphite

The loader runs StatsD client and, therefore, can be easily used in conjunction with [Grafana](https://grafana.com) or Graphite.

The local UDP port that `aisloader` uses to send its own collected statistics is the default 8125 (configurable via `-statsdport=<statsd daemon port number>`).
