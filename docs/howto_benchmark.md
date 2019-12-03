## Table of Contents
- [AIS Loader](#ais-loader)
   - [Examples](#examples)
   - [Command-line options](#command-line-options)
   - [Cluster Under Test](#cluster-under-test)
   - [AIS vs Cloud-based bucket](#ais-vs-cloud-based-bucket)
   - [Duration](#duration)
   - [Write vs Read](#write-vs-read)
   - [Read range](#read-range)
   - [Cleanup](#cleanup)
   - [Object size](#object-size)
   - [Grafana and Graphite](#grafana-and-graphite)

## AIS Loader

AIS Loader (`aisloader`) is a tool to measure storage performance. It's a load generator that has been developed (and is currently used) to benchmark and stress-test AIStore(tm) but can be easily extended for any S3-compatible backend.

For usage, run: `aisloader` or `aisloader usage` or `aisloader --help`.

To get started, cd to [aisloader home](/bench/aisloader/) and run:

```shell
$ ./install.sh
$ $GOPATH/bin/aisloader
```
### Examples

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

1. Destroy existing ais bucket. If the bucket is Cloud-based, delete all objects:
```shell
# aisloader -bucket=nvais -duration 0s -totalputsize=0
```

2. Time-based 100% PUT into ais bucket. Upon exit the bucket is emptied (by default):
```shell
# aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais
```

3. Timed (for 1h) 100% GET from a Cloud bucket, no cleanup:
```shell
aisloader -bucket=nvaws -duration 1h -numworkers=30 -pctput=0 -provider=cloud -cleanup=false
```

4. Mixed 30%/70% PUT and GET of variable-size objects to/from a Cloud bucket. PUT will generate random object names and is limited by the 10GB total size. Cleanup is not disabled, which means that upon completion all generated objects will be deleted:
```shell
# aisloader -bucket=nvaws -duration 0s -numworkers=3 -minsize=1024 -maxsize=1MB -pctput=30 -provider=cloud -totalputsize=10G
```

5. PUT 1GB total into an ais bucket with cleanup disabled, object size = 1MB, duration unlimited:
```shell
# aisloader -bucket=nvais -cleanup=false -totalputsize=1G -duration=0 -minsize=1MB -maxsize=1MB -numworkers=8 -pctput=100 -provider=ais
```

6. 100% GET from an ais bucket:
```shell
# aisloader -bucket=nvais -duration 5s -numworkers=3 -pctput=0 -provider=ais
```

7. PUT 2000 objects named as `aisloader/hex({0..2000}{loaderid}):
```shell
# aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -objNamePrefix="aisloader"
```

8. Use random object names and loaderID to report statistics:
```shell
# aisloader -loaderid=10
```

9. PUT objects with random name generation being based on the specified loaderID and the total number of concurrent aisloaders:
```shell
# aisloader -loaderid=10 -loadernum=20
```

10. Same as above except that loaderID is computed by the aisloader as hash(loaderstring) & 0xff:
```shell
# aisloader -loaderid=loaderstring -loaderidhashlen=8
```

11. Print loaderID and exit (all 3 examples below) with the resulting loaderID shown on the right:
```shell
# aisloader -getloaderid (0x0)
# aisloader -loaderid=10 -getloaderid (0xa)
# aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid (0xdb)
```

### Command-line Options

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

| Command-line option | Description |
| --- | --- |
| -batchsize | Batch size to list and delete (default 100) |
| -provider | ais - for AIS, cloud - for Cloud bucket; other supported values include "gcp" and "aws", for Amazon and Google clouds, respectively (default "ais") |
| -bprops | JSON string formatted as per the SetBucketProps API and containing bucket properties to apply |
| -bucket | Bucket name (default "nvais") |
| -check-statsd | true: prior to benchmark make sure that StatsD is reachable |
| -cleanup | true: remove all created objects upon benchmark termination (default true) |
| -dry-run | show the configuration and parameters that aisloader will use |
| -duration | Benchmark duration (0 - run forever or until Ctrl-C, default 1m). Note that if both duration and totalputsize are zeros, aisloader will have nothing to do |
| -getconfig | true: generate control plane load by reading AIS proxy configuration (that is, instead of reading/writing data exercise control path) |
| -getloaderid | true: print stored/computed unique loaderID aka aisloader identifier and exit |
| -ip | AIS proxy/gateway IP address or hostname (default "localhost") |
| -json | true: print the output in JSON |
| -loaderid | ID to identify a loader among multiple concurrent instances (default "0") |
| -loaderidhashlen | Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum |
| -loadernum | total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen |
| -maxputs | Maximum number of objects to PUT |
| -maxsize | Maximum object size (with or without multiplicative suffix K, MB, GiB, etc.) |
| -minsize | Minimum object size (with or without multiplicative suffix K, MB, GiB, etc.) |
| -numworkers | Number of goroutine workers operating on AIS in parallel (default 10) |
| -pctput | Percentage of PUTs in the aisloader-generated workload |
| -port | Percentage of PUTs in the aisloader-generated workload |
| -randomname | true: generate object names of 32 random characters. This option is ignored when loadernum is defined (default true) |
| -readertype | Type of reader: sg(default) | file | rand (default "sg") |
| -readlen | Read range length (can contain multiplicative suffix; 0 - GET full object) |
| -readoff | Read range offset (can contain multiplicative suffix K, MB, GiB, etc.) |
| -seed | Random seed to achieve deterministic reproducible results (0 - use current time in nanoseconds) |
| -stats-output | filename to log statistics (empty string translates as standard output (default) |
| -statsdip | StatsD IP address or hostname (default "localhost") |
| -statsdport | StatsD UDP port (default 8125) |
| -statsinterval | Interval in seconds to print performance counters; 0 - disabled (default 10 seconds) |
| -subdir | Virtual destination directory for all aisloader-generated objects |
| -tmpdir | Local directory to store temporary files (default "/tmp/ais") |
| -totalputsize | Stop PUT workload once cumulative PUT size reaches or exceeds this value (can contain standard multiplicative suffix K, MB, GiB, etc.), 0 - unlimited |
| -uniquegets | true: GET objects randomly and equally. Meaning, make sure *not* to GET some objects more frequently than the others (default true) |
| -usage | Show command-line options, usage, and examples |
| -verifyhash | checksum-validate GET: recompute object checksums and validate it against the one received with the GET metadata |

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

### AIS vs Cloud-based bucket

In the example above, the "nothing to read" indicates that `aistore` requires a bucket to operate upon. Use the switch '-ais' to select between ais bucket (`-ais=true`) or Cloud-based one (`-ais=false`).

> Terminology: the term *ais bucket* simply means that the bucket in question does **not** cache (or tier) 3rd party Cloud and is not backed by the namesake Cloud bucket. AIS buckets are distributed across the entire AIS cluster. All the [supported storage services](/docs/storage_svcs.md) equally apply to both sorts of buckets.

Note that aisloader **will create an ais bucket if it does not exist**.

Further, the name of the bucket is set via the option `-bucket=<bucket name>`.
For instance:

```
$ aisloader -ip=example.com -port=8080 -bucket=abc -provider=ais
```

### Duration

The loads can run for a given period of time (option `-duration <duration>`) or until the specified amount of data is generated (option `-totalputsize=<total size in KBs>`).

If both options are provided the test finishes on the whatever-comes-first basis.

Example 100% write into the bucket "abc" for 2 hours:

```shell
$ aisloader -bucket=abc -provider=ais -duration 2h -totalputsize=4000000 -pctput=100
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

The first line in this example above fills the bucket "abc" with 16MiB of random data. The second - uses existing data to test read performance for 1 hour, and then removes all data.

If you just need to cleanup old data prior to running a test, run the loader with 0 (zero) total put size and zero duration:

```shell
$ aisloader -bucket=<bucket to cleanup> -duration 0s -totalputsize=0
```

### Object size

For the PUT workload the loader generates randomly-filled objects. But what about object sizing?

By default, object sizes are randomly selected as well in the range between 1MiB and 1GiB. To set preferred (or fixed) object size(s), use the options `-minsize=<minimal object size in KiB>` and `-maxsize=<maximum object size in KiB>`

### Setting bucket properties

Before starting a test, it is possible to set `mirror` or `EC` properties on a bucket (for background, please see [storage services](/docs/storage_svcs.md)).

> For background on local mirroring and erasure coding (EC), please see [storage services](/docs/storage_svcs.md).

To achieve that, use the option `-bprops`. For example:

```shell
$ aisloader -bucket=abc -pctput=0 -cleanup=false -duration 10s -bprops='{"mirror": {"copies": 2, "enabled": false, "util_thresh": 5}, "ec": {"enabled": false, "data_slices": 2, "parity_slices": 2}}'
```

The above example shows the values that are globally default as of the AIS version 2.0. You can omit the defaults and specify only those values that you'd want to change. For instance, to enable erasure coding on the bucket "abc":

```shell
$ aisloader -bucket=abc -duration 10s -bprops='{"ec": {"enabled": true}}'
```

This example sets the number of data and parity slices to 2 which, in turn, requires the cluster to have at least 5 target nodes: 2 for data slices, 2 for parity slices and one for the original object.

> As of v2.0, once erasure coding is enabled, its properties `data_slices` and `parity_slices` cannot be changed on the fly.

The following sequence populates a bucket configured for both local mirroring and erasure coding, and then reads from it for 1h:

```shell
# Fill bucket
$ aisloader -bucket=abc -cleanup=false -pctput=100 -duration 100m -bprops='{"mirror": {"enabled": true}, "ec": {"enabled": true}}'

# Read
$ aisloader -bucket=abc -cleanup=false -pctput=0 -duration 1h
```

### Grafana and Graphite

The loader runs StatsD client and, therefore, can be easily used in conjunction with [Grafana](https://grafana.com) or Graphite.

The local UDP port that `aisloader` uses to send its own collected statistics is the default 8125 (configurable via `-statsdport=<statsd daemon port number>`).
