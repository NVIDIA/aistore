---
layout: post
title: AISLOADER
permalink: bench/aisloader
redirect_from:
 - bench/aisloader/README.md/
---

# AIS Loader

AIS Loader (`aisloader`) is a tool to measure storage performance. It is a load generator that we constantly use to benchmark and stress-test [AIStore](https://github.com/NVIDIA/aistore). The tool was written in such a way that it can be easily extended to benchmark any S3-compatible backend.

In addition, there's a certain AI "angle": AIS Loader can generate (synthetic) workloads that mimic training and inference workloads - the capability that allows to run benchmarks in isolation (which is often preferable), and also avoid compute-side bottlenecks if there are any.

## Table of Contents

- [Setup](#Setup)
- [Command line Options](#command-line-options)
    - [Often used options explanation](#often-used-options-explanation)
- [Examples](#examples)
- [Collecting stats](#collecting-stats)
    - [Grafana](#grafana)

## Setup

To get started, go to root directory and run:

```console
$ make aisloader
```

For usage, run: `aisloader` or `aisloader usage` or `aisloader --help`.

## Command-line Options

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

### Options via AIS Loader flags

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -batchsize | `int` | Batch size to list and delete | `100` |
| -bprops | `json` | JSON string formatted as per the SetBucketProps API and containing bucket properties to apply | `""` |
| -bucket | `string` | Bucket name. Bucket will be created if doesn't exist. If empty, aisloader generates a new random bucket name | `""` |
| -check-statsd | `bool` | true: prior to benchmark make sure that StatsD is reachable | `false` |
| -cleanup | `bool` | true: remove all created objects upon benchmark termination | `true` |
| -dry-run | `bool` | show the configuration and parameters that aisloader will use | `false` |
| -duration | `string`, `int` | Benchmark duration (0 - run forever or until Ctrl-C, default 1m). Note that if both duration and totalputsize are zeros, aisloader will have nothing to do | `1m` |
| -getconfig | `bool` | true: generate control plane load by reading AIS proxy configuration (that is, instead of reading/writing data exercise control path) | `false` |
| -getloaderid | `bool` | true: print stored/computed unique loaderID aka aisloader identifier and exit | `false` |
| -ip | `string` | AIS proxy/gateway IP address or hostname | `localhost` |
| -json | `bool` | true: print the output in JSON | `false` |
| -loaderid | `string` | ID to identify a loader among multiple concurrent instances | `0` |
| -loaderidhashlen | `int` | Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum | `0` |
| -loadernum | `int` | total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen | `0` |
| -maxputs | `int` | Maximum number of objects to PUT | `0` |
| -maxsize | `int` | Maximal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1GiB` |
| -minsize | `int` | Minimal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1MiB` |
| -numworkers | `int` | Number of goroutine workers operating on AIS in parallel | `10` |
| -pctput | `int` | Percentage of PUTs in the aisloader-generated workload | `0` |
| -port | `int` | Port number for proxy server | `8080` |
| -provider | `string` | ais - for AIS, cloud - for Cloud bucket; other supported values include "gcp" and "aws", for Amazon and Google clouds, respectively | `ais` |
| -putshards | `int` | Spread generated objects over this many subdirectories (max 100k) | `0` |
| -randomname | `bool` | true: generate object names of 32 random characters. This option is ignored when loadernum is defined | `true` |
| -readertype | `string` | Type of reader: sg(default). Available: `sg`, `file`, `rand`, `tar` | `sg` |
| -readlen | `string`, `int` | Read range length, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -readoff | `string`, `int` | Read range offset (can contain multiplicative suffix K, MB, GiB, etc.) | `""` |
| -seed | `int` | Random seed to achieve deterministic reproducible results (0 - use current time in nanoseconds) | `0` |
| -stats-output | `string` | filename to log statistics (empty string translates as standard output (default) | `""` |
| -statsdip | `string` | StatsD IP address or hostname | `localhost` |
| -statsdport | `int` | StatsD UDP port | `8125` |
| -statsdprobe | `bool` | Test-probe StatsD server prior to benchmarks | `true` |
| -statsinterval | `int` | Interval in seconds to print performance counters; 0 - disabled | `10` |
| -subdir | `string` | Virtual destination directory for all aisloader-generated objects | `""` |
| -tmpdir | `string` | Local directory to store temporary files | `/tmp/ais` |
| -timeout | `string` | Client HTTP timeout; `0` = infinity) | `10m` |
| -etl | `string` | Built-in ETL, one-of: `tar2tf`, `md5`, or `echo`. Each object that `aisloader` GETs undergoes the selected transformation. See also: `-etl-spec` option. | `""` |
| -etl-spec | `string` | Custom ETL specification (pathname). Must be compatible with Kubernetes Pod specification. Each object that `aisloader` GETs will undergo this user-defined transformation. See also: `-etl` option. | `""` |
| -totalputsize | `string`, `int` | Stop PUT workload once cumulative PUT size reaches or exceeds this value, can contain [multiplicative suffix](#bytes-multiplicative-suffix), 0 = no limit | `0` |
| -uniquegets | `bool` | true: GET objects randomly and equally. Meaning, make sure *not* to GET some objects more frequently than the others | `true` |
| -usage | `bool` | Show command-line options, usage, and examples | `false` |
| -verifyhash | `bool` | checksum-validate GET: recompute object checksums and validate it against the one received with the GET metadata | `true` |

### Options via environment variables

| Environment Variable | Type | Description |
| `AIS_ENDPOINT` | `string` | Address of a cluster which aisloader will generate load on. Overrides `ip` and `port` flags. |

### Often used options explanation

#### Duration

The loads can run for a given period of time (option `-duration <duration>`) or until the specified amount of data is generated (option `-totalputsize=<total size in KBs>`).

If both options are provided the test finishes on the whatever-comes-first basis.

Example 100% write into the bucket "abc" for 2 hours:

```console
$ aisloader -bucket=abc -provider=ais -duration 2h -totalputsize=4000000 -pctput=100
```

The above will run for two hours or until it writes around 4GB data into the bucket, whatever comes first.

#### Write vs Read

You can choose a percentage of writing (versus reading) by setting the option `-pctput=<put percentage>`.

Example with a mixed PUT=30% and GET=70% load:

```console
$ aisloader -bucket=abc -duration 5m -pctput=30
```

Example 100% PUT:

```console
$ aisloader -bucket=abc -duration 5m -pctput=100
```

The duration in both examples above is set to 5 minutes.

> To test 100% read (`-pctput=0`), make sure to fill the bucket beforehand.

#### Read range

The loader can read the entire object (default) **or** a range of object bytes.

To set the offset and length to read, use option `-readoff=<read offset (in bytes)>` and `readlen=<length to read (in bytes)>`.

For convenience, both options support size suffixes: `k` - for KiB, `m` - for MiB, and `g` - for GiB.

Example that reads a 32MiB segment at 1KB offset from each object stored in the bucket "abc":

```console
$ aisloader -bucket=abc -duration 5m -cleanup=false -readoff=1024 -readlen=32m
```

The test (above) will run for 5 minutes and will not "cleanup" after itself (next section).

#### Cleanup

By default, `aisloader` deletes all the data after completing its run. But what if, for instance, you'd want to test reads (`pctput=0`) after having populated the cluster via 100% PUT.

In this and similar cases, disable automatic cleanup by passing the option `cleanup=false`.

Example:

```console
$ aisloader -bucket=abc -pctput=100 -totalputsize=16348 -cleanup=false
$ aisloader -bucket=abc -duration 1h -pctput=0 -cleanup=true
```

The first line in this example above fills the bucket "abc" with 16MiB of random data. The second - uses existing data to test read performance for 1 hour, and then removes all data.

If you just need to clean up old data prior to running a test, run the loader with 0 (zero) total put size and zero duration:

```console
$ aisloader -bucket=<bucket to cleanup> -duration 0s -totalputsize=0
```

#### Object size

For the PUT workload the loader generates randomly-filled objects. But what about object sizing?

By default, object sizes are randomly selected as well in the range between 1MiB and 1GiB. To set preferred (or fixed) object size(s), use the options `-minsize=<minimal object size in KiB>` and `-maxsize=<maximum object size in KiB>`

#### Setting bucket properties

Before starting a test, it is possible to set `mirror` or `EC` properties on a bucket (for background, please see [storage services](/docs/storage_svcs.md)).

> For background on local mirroring and erasure coding (EC), please see [storage services](/docs/storage_svcs.md).

To achieve that, use the option `-bprops`. For example:

```console
$ aisloader -bucket=abc -pctput=0 -cleanup=false -duration 10s -bprops='{"mirror": {"copies": 2, "enabled": false, "util_thresh": 5}, "ec": {"enabled": false, "data_slices": 2, "parity_slices": 2}}'
```

The above example shows the values that are globally default. You can omit the defaults and specify only those values that you'd want to change. For instance, to enable erasure coding on the bucket "abc":

```console
$ aisloader -bucket=abc -duration 10s -bprops='{"ec": {"enabled": true}}'
```

This example sets the number of data and parity slices to 2 which, in turn, requires the cluster to have at least 5 target nodes: 2 for data slices, 2 for parity slices and one for the original object.

> Once erasure coding is enabled, its properties `data_slices` and `parity_slices` cannot be changed on the fly.

The following sequence populates a bucket configured for both local mirroring and erasure coding, and then reads from it for 1h:

```console
# Fill bucket
$ aisloader -bucket=abc -cleanup=false -pctput=100 -duration 100m -bprops='{"mirror": {"enabled": true}, "ec": {"enabled": true}}'

# Read
$ aisloader -bucket=abc -cleanup=false -pctput=0 -duration 1h
```

### Bytes Multiplicative Suffix

Parameters in AISLoader that represent the number of bytes can be specified with a multiplicative suffix.
For example: `8M` would specify 8 MiB.
The following multiplicative suffixes are supported: 't' or 'T' - TiB 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB.
Note that this is entirely optional, and therefore an input such as `300` will be interpreted as 300 Bytes.

## Examples

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

1. Create a 10-seconds load of 50% PUT and 50% GET requests:

```console
$ aisloader -bucket=my_ais_bucket -duration=10s -pctput=50 -provider=ais
Found 0 existing objects
Run configuration:
{
    "proxy": "http://172.50.0.2:8080",
    "provider": "ais",
    "bucket": "my_ais_bucket",
    "duration": "10s",
    "put upper bound": 0,
    "put %": 50,
    "minimal object size in Bytes": 1024,
    "maximal object size in Bytes": 1048576,
    "# workers": 1,
    "stats interval": "10s",
    "backed by": "sg",
    "cleanup": true
}

Actual run duration: 10.313689487s

Time      OP    Count                 	Total Bytes           	Latency(min, avg, max)              	Throughput            	Error
01:52:52  Put   26                    	11.19GB               	296.39ms   5.70s      14.91s        	639.73MB              	0
01:52:52  Get   16                    	3.86GB                	58.89ms    220.20ms   616.72ms      	220.56MB              	0
01:52:52  CFG   0                     	0B                    	0.00ms     0.00ms     0.00ms        	0B                    	0
01:52:52 Clean up ...
01:52:54 Clean up done
```

2. Time-based 100% PUT into ais bucket. Upon exit the bucket is emptied (by default):

```console
$ aisloader -bucket=nvais -duration 10s -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais
```

3. Timed (for 1h) 100% GET from a Cloud bucket, no cleanup:

```console
$ aisloader -bucket=nvaws -duration 1h -numworkers=30 -pctput=0 -provider=cloud -cleanup=false
```

4. Mixed 30%/70% PUT and GET of variable-size objects to/from a Cloud bucket. PUT will generate random object names and is limited by the 10GB total size. Cleanup is not disabled, which means that upon completion all generated objects will be deleted:

```console
$ aisloader -bucket=nvaws -duration 0s -numworkers=3 -minsize=1024 -maxsize=1MB -pctput=30 -provider=cloud -totalputsize=10G
```

5. PUT 1GB total into an ais bucket with cleanup disabled, object size = 1MB, duration unlimited:

```console
$ aisloader -bucket=nvais -cleanup=false -totalputsize=1G -duration=0 -minsize=1MB -maxsize=1MB -numworkers=8 -pctput=100 -provider=ais
```

6. 100% GET from an ais bucket:

```console
$ aisloader -bucket=nvais -duration 5s -numworkers=3 -pctput=0 -provider=ais
```

7. PUT 2000 objects named as `aisloader/hex({0..2000}{loaderid})`:

```console
$ aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -objNamePrefix="aisloader"
```

8. Use random object names and loaderID to report statistics:

```console
$ aisloader -loaderid=10
```

9. PUT objects with random name generation being based on the specified loaderID and the total number of concurrent aisloaders:

```console
$ aisloader -loaderid=10 -loadernum=20
```

10. Same as above except that loaderID is computed by the aisloader as `hash(loaderstring) & 0xff`:

```console
$ aisloader -loaderid=loaderstring -loaderidhashlen=8
```

11. Print loaderID and exit (all 3 examples below) with the resulting loaderID shown on the right:

```console
$ aisloader -getloaderid (0x0)
$ aisloader -loaderid=10 -getloaderid (0xa)
$ aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid (0xdb)
```

12. Destroy existing ais bucket. If the bucket is Cloud-based, delete all objects:

```console
$ aisloader -bucket=nvais -duration 0s -totalputsize=0
```

13. Generate load on a cluster listening on custom IP address and port:

```console
$ aisloader -ip="example.com" -port=8080
```

14. Generate load on a cluster listening on custom IP address and port from environment variable:

```console
$ AIS_ENDPOINT="examples.com:8080" aisloader
```

15. Use HTTPS when connecting to a cluster:

```console
$ aisloader -ip="https://localhost" -port=8080
```

16. PUT TAR files with random files inside into a cluster:

```console
$ aisloader -bucket=my_ais_bucket -duration=10s -pctput=100 -provider=ais -readertype=tar
```

17. Generate load on `tar2tf` ETL. New ETL is started and then stopped at the end. TAR files are PUT to the cluster. Only available when cluster is deployed on Kubernetes.

```console
$ aisloader -bucket=my_ais_bucket -duration=10s -pctput=100 -provider=ais -readertype=tar -etl=tar2tf
```

## Collecting stats

Aisloader allows you to collect statistics with Graphite using StatsD. Aisloader will try to connect
to provided StatsD server (see: `statsdip` and `statsdport` options). Once the
connection is established the statistics from aisloader are send in the following
format:

```
<metric_type>.aisloader.<hostname>-<loaderid>.<metric>
```

* `metric_type` - can be: `gauge`, `timer`, `counter`
* `hostname` - is the hostname of the machine on which the loader is ran
* `loaderid` - see: `-loaderid` option
* `metric` - can be: `latency.*`, `get.*`, `put.*` (see: [aisloader metrics](/docs/metrics.md#ais-loader-metrics))

### Grafana

Grafana helps visualize the collected statistics. It is convenient to use and
provides numerous tools to measure and calculate different metrics.

We provide simple [script](/deploy/dev/local/deploy_grafana.sh) which allows you to set
up the Graphite and Grafana servers which run inside separate dockers. To add
new dashboards and panels, please follow: [grafana tutorial](http://docs.grafana.org/guides/getting_started/).

When selecting a series in panel view, it should be in the format: `stats.aisloader.<loader>.*`.
Remember that metrics will not be visible (and you will not be able to select
them) until you start the loader.
