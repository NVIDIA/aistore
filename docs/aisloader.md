---
layout: post
title: AISLOADER
permalink: /docs/aisloader
redirect_from:
 - /aisloader.md/
 - /docs/aisloader.md/
---

# AIS Loader

AIS Loader ([`aisloader`](/bench/tools/aisloader)) is a tool to measure storage performance. It is a load generator that we constantly use to benchmark and stress-test [AIStore](https://github.com/NVIDIA/aistore) or any S3-compatible backend.

In fact, aisloader can list, write, and read S3(**) buckets _directly_, which makes it quite useful, convenient, and easy to use benchmark to compare storage performance **with** aistore in front of S3 and **without**.

> (**) `aisloader` can be further easily extended to work directly with any Cloud storage provider including, but not limited to, aistore-supported GCP and Azure.

In addition, `aisloader` generates synthetic workloads that mimic training and inference workloads - the capability that allows to run benchmarks in isolation (which is often preferable) avoiding compute-side bottlenecks (if any) and associated complexity.

There's a large set of command-line switches that allow to realize almost any conceivable workload, with basic permutations always including:

* number of workers
* read and write sizes
* read and write ratios

Detailed protocol-level tracing statistics are also available - see [HTTP tracing](#http-tracing) section below for brief introduction.

## Table of Contents

- [Setup](#Setup)
- [Command line Options](#command-line-options)
    - [Often used options explanation](#often-used-options-explanation)
- [Environment variables](#environment-variables)
- [Examples](#examples)
- [Collecting stats](#collecting-stats)
    - [Grafana](#grafana)
- [HTTP tracing](#http-tracing)
- [AISLoader-Composer](#aisloader-composer)
- [References](#references)

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
| -cached | `bool` | list in-cluster objects - only those objects from a remote bucket that are present ("cached") | `false` |
| -cksum-type | `string` | Checksum type to use for PUT object requests | `xxhash`|
| -cleanup | `bool` | when true, remove bucket upon benchmark termination | `n/a` (required) |
| -dry-run | `bool` | show the entire set of parameters that aisloader will use when actually running | `false` |
| -duration | `string`, `int` | Benchmark duration (0 - run forever or until Ctrl-C, default 1m). Note that if both duration and totalputsize are zeros, aisloader will have nothing to do | `1m` |
| -epochs | `int` |  Number of "epochs" to run whereby each epoch entails full pass through the entire listed bucket | `1`|
| -etl | `string` | Built-in ETL, one-of: `tar2tf`, `md5`, or `echo`. Each object that `aisloader` GETs undergoes the selected transformation. See also: `-etl-spec` option. | `""` |
| -etl-spec | `string` | Custom ETL specification (pathname). Must be compatible with Kubernetes Pod specification. Each object that `aisloader` GETs will undergo this user-defined transformation. See also: `-etl` option. | `""` |
| -getconfig | `bool` | when true, generate control plane load by reading AIS proxy configuration (that is, instead of reading/writing data exercise control path) | `false` |
| -getloaderid | `bool` | when true, print stored/computed unique loaderID aka aisloader identifier and exit | `false` |
| -ip | `string` | AIS proxy/gateway IP address or hostname | `localhost` |
| -json | `bool` | when true, print the output in JSON | `false` |
| -loaderid | `string` | ID to identify a loader among multiple concurrent instances | `0` |
| -loaderidhashlen | `int` | Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum | `0` |
| -loadernum | `int` | total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen | `0` |
| -maxputs | `int` | Maximum number of objects to PUT | `0` |
| -maxsize | `int` | Maximal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1GiB` |
| -minsize | `int` | Minimal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1MiB` |
| -numworkers | `int` | Number of goroutine workers operating on AIS in parallel | `10` |
| -pctput | `int` | Percentage of PUTs in the aisloader-generated workload | `0` |
| -latest | `bool` | When true, check in-cluster metadata and possibly GET the latest object version from the associated remote bucket | `false` |
| -port | `int` | Port number for proxy server | `8080` |
| -provider | `string` | ais - for AIS, cloud - for Cloud bucket; other supported values include "gcp" and "aws", for Amazon and Google clouds, respectively | `ais` |
| -putshards | `int` | Spread generated objects over this many subdirectories (max 100k) | `0` |
| -quiet | `bool` | When starting to run, do not print command line arguments, default settings, and usage examples | `false` |
| -randomname | `bool` | when true, generate object names of 32 random characters. This option is ignored when loadernum is defined | `true` |
| -readertype | `string` | Type of reader: sg(default). Available: `sg`, `file`, `rand`, `tar` | `sg` |
| -readlen | `string`, `int` | Read range length, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -readoff | `string`, `int` | Read range offset (can contain multiplicative suffix K, MB, GiB, etc.) | `""` |
| -s3endpoint | `string` | S3 endpoint to read/write S3 bucket directly (with no aistore) | `""` |
| -s3profile | `string` | Other then default S3 config profile referencing alternative credentials | `""` |
| -seed | `int` | Random seed to achieve deterministic reproducible results (0 - use current time in nanoseconds) | `0` |
| -skiplist | `bool` | Whether to skip listing objects in a bucket before running PUT workload | `false` |
| -filelist | `string` | Local or locally accessible text file file containing object names (for subsequent reading) | `""` |
| -stats-output | `string` | filename to log statistics (empty string translates as standard output (default) | `""` |
| -statsdip | `string` | StatsD IP address or hostname | `localhost` |
| -statsdport | `int` | StatsD UDP port | `8125` |
| -statsdprobe | `bool` | Test-probe StatsD server prior to benchmarks | `true` |
| -statsinterval | `int` | Interval in seconds to print performance counters; 0 - disabled | `10` |
| -subdir | `string` | For GET requests, `-subdir` is a prefix that may or may not be an actual [virtual directory](/docs/howto_virt_dirs.md). For PUT, `-subdir` is a virtual destination directory for all aisloader-generated objects. See closely related [CLI](/docs/cli/object.md) `--prefix` option. | `""` |
| -test-probe | `bool`| Test StatsD server prior to running benchmarks | `false` |
| -timeout | `string` | Client HTTP timeout; `0` = infinity) | `10m` |
| -tmpdir | `string` | Local directory to store temporary files | `/tmp/ais` |
| -tokenfile | `string` | Authentication token (FQN) | `""`|
| -totalputsize | `string`, `int` | Stop PUT workload once cumulative PUT size reaches or exceeds this value, can contain [multiplicative suffix](#bytes-multiplicative-suffix), 0 = no limit | `0` |
| -trace-http | `bool` | Trace HTTP latencies (see [HTTP tracing](#http-tracing)) | `false` |
| -uniquegets | `bool` | when true, GET objects randomly and equally. Meaning, make sure *not* to GET some objects more frequently than the others | `true` |
| -usage | `bool` | Show command-line options, usage, and examples | `false` |
| -verifyhash | `bool` | checksum-validate GET: recompute object checksums and validate it against the one received with the GET metadata | `true` |

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
$ aisloader -bucket=ais://abc -duration 5m -pctput=30 -cleanup=true
```

Example 100% PUT:

```console
$ aisloader -bucket=abc -duration 5m -pctput=100 -cleanup=true
```

The duration in both examples above is set to 5 minutes.

> To test 100% read (`-pctput=0`), make sure to fill the bucket beforehand.

#### Read range

The loader can read the entire object (default) **or** a range of object bytes.

To set the offset and length to read, use option `-readoff=<read offset (in bytes)>` and `readlen=<length to read (in bytes)>`.

For convenience, both options support size suffixes: `k` - for KiB, `m` - for MiB, and `g` - for GiB.

Example that reads a 32MiB segment at 1KB offset from each object stored in the bucket "abc":

```console
$ aisloader -bucket=ais://abc -duration 5m -cleanup=false -readoff=1024 -readlen=32m
```

The test (above) will run for 5 minutes and will not "cleanup" after itself (next section).

#### Cleanup

**NOTE**: `-cleanup` is a mandatory option defining whether to destroy bucket upon completion of the benchmark.

The option must be specified in the command line.

Example:

```console
$ aisloader -bucket=ais://abc -pctput=100 -totalputsize=16348 -cleanup=false
$ aisloader -bucket=ais://abc -duration 1h -pctput=0 -cleanup=true
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
$ aisloader -bucket=ais://abc -pctput=0 -cleanup=false -duration 10s -bprops='{"mirror": {"copies": 2, "enabled": false}, "ec": {"enabled": false, "data_slices": 2, "parity_slices": 2}}'
```

The above example shows the values that are globally default. You can omit the defaults and specify only those values that you'd want to change. For instance, to enable erasure coding on the bucket "abc":

```console
$ aisloader -bucket=ais://abc -duration 1h -bprops='{"ec": {"enabled": true}}' -cleanup=false
```

This example sets the number of data and parity slices to 2 which, in turn, requires the cluster to have at least 5 target nodes: 2 for data slices, 2 for parity slices and one for the original object.

> Once erasure coding is enabled, its properties `data_slices` and `parity_slices` cannot be changed on the fly.

> Note that (n `data_slices`, m `parity_slices`) erasure coding requires at least (n + m + 1) target nodes in a cluster.

> Even though erasure coding and/or mirroring can be enabled/disabled and otherwise reconfigured at any point in time, specifically for the purposes of running benchmarks it is generally recommended to do it once _prior_ to writing any data to the bucket in question.

The following sequence populates a bucket configured for both local mirroring and erasure coding, and then reads from it for 1h:

```console
# Fill bucket
$ aisloader -bucket=ais://abc -cleanup=false -pctput=100 -duration 100m -bprops='{"mirror": {"enabled": true}, "ec": {"enabled": true}}'

# Read
$ aisloader -bucket=abc -cleanup=false -pctput=0 -duration 1h
```

### Bytes Multiplicative Suffix

Parameters in `aisLoader` that represent the number of bytes can be specified with a multiplicative suffix.
For example: `8M` would specify 8 MiB.
The following multiplicative suffixes are supported: 't' or 'T' - TiB 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB.
Note that this is entirely optional, and therefore an input such as `300` will be interpreted as 300 Bytes.

## Environment variables

| Environment Variable | Type | Description |
| -- | -- | -- |
| `AIS_ENDPOINT` | `string` | Cluster's endpoint: http or https address of any aistore gateway in this cluster. Overrides `ip` and `port` flags. |

To state the same slightly differently, cluster endpoint can be defined in two ways:

* as (plain) http://ip:port address, whereby '--ip' and '--port' are command-line options.
* via `AIS_ENDPOINT` environment universally supported across all AIS clients, e.g.:

```console
$ export AIS_ENDPOINT=https://10.07.56.68:51080
```

In addition, environment can be used to specify client-side TLS (aka, HTTPS) configuration:

| var name | description |
| -- | -- |
| `AIS_CRT`             | X.509 certificate |
| `AIS_CRT_KEY`         | X.509 certificate's private key |
| `AIS_CLIENT_CA`       | Certificate authority that authorized (signed) the certificate |
| `AIS_SKIP_VERIFY_CRT` | when true, skip X.509 cert verification (usually enabled to circumvent limitations of self-signed certs) |

See also:

* [HTTPS: loading, reloading, and generating certificates; switching cluster between HTTP and HTTPS](/docs/https.md)

## Examples

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

**1**. Create a 10-seconds load of 50% PUT and 50% GET requests:

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
        "maximum object size in Bytes": 1048576,
        "worker count": 1,
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

**2**. Time-based 100% PUT into ais bucket. Upon exit the bucket is destroyed:

    ```console
    $ aisloader -bucket=nvais -duration 10s -cleanup=true -numworkers=3 -minsize=1K -maxsize=1K -pctput=100 -provider=ais
    ```

**3**. Timed (for 1h) 100% GET from a Cloud bucket, no cleanup:

    ```console
    $ aisloader -bucket=aws://nvaws -duration 1h -numworkers=30 -pctput=0 -cleanup=false
    ```

**4**. Mixed 30%/70% PUT and GET of variable-size objects to/from a Cloud bucket. PUT will generate random object names and is limited by the 10GB total size. Cleanup enabled - upon completion all generated objects and the bucket itself will be deleted:

    ```console
    $ aisloader -bucket=s3://nvaws -duration 0s -cleanup=true -numworkers=3 -minsize=1024 -maxsize=1MB -pctput=30 -totalputsize=10G
    ```

**5**. PUT 1GB total into an ais bucket with cleanup disabled, object size = 1MB, duration unlimited:

    ```console
    $ aisloader -bucket=nvais -cleanup=false -totalputsize=1G -duration=0 -minsize=1MB -maxsize=1MB -numworkers=8 -pctput=100 -provider=ais
    ```

**6**. 100% GET from an ais bucket:

    ```console
    $ aisloader -bucket=nvais -duration 5s -numworkers=3 -pctput=0 -provider=ais -cleanup=false
    ```

**7**. PUT 2000 objects named as `aisloader/hex({0..2000}{loaderid})`:

    ```console
    $ aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000 -objNamePrefix="aisloader" -cleanup=false
    ```

**8**. Use random object names and loaderID to report statistics:

    ```console
    $ aisloader -loaderid=10
    ```

**9**. PUT objects with random name generation being based on the specified loaderID and the total number of concurrent aisloaders:

    ```console
    $ aisloader -loaderid=10 -loadernum=20
    ```

**10**. Same as above except that loaderID is computed by the aisloader as `hash(loaderstring) & 0xff`:

    ```console
    $ aisloader -loaderid=loaderstring -loaderidhashlen=8
    ```

**11**. Print loaderID and exit (all 3 examples below) with the resulting loaderID shown on the right:

    ```console
    $ aisloader -getloaderid (0x0)
    $ aisloader -loaderid=10 -getloaderid (0xa)
    $ aisloader -loaderid=loaderstring -loaderidhashlen=8 -getloaderid (0xdb)
    ```

**12**. Destroy existing ais bucket. If the bucket is Cloud-based, delete all objects:

    ```console
    $ aisloader -bucket=nvais -duration 0s -totalputsize=0 -cleanup=true
    ```

**13**. Generate load on a cluster listening on custom IP address and port:

    ```console
    $ aisloader -ip="example.com" -port=8080
    ```

**14**. Generate load on a cluster listening on custom IP address and port from environment variable:

    ```console
    $ AIS_ENDPOINT="examples.com:8080" aisloader
    ```

**15**. Use HTTPS when connecting to a cluster:

    ```console
    $ aisloader -ip="https://localhost" -port=8080
    ```

**16**. PUT TAR files with random files inside into a cluster:

    ```console
    $ aisloader -bucket=my_ais_bucket -duration=10s -pctput=100 -provider=ais -readertype=tar
    ```

**17**. Generate load on `tar2tf` ETL. New ETL is started and then stopped at the end. TAR files are PUT to the cluster. Only available when cluster is deployed on Kubernetes.

    ```console
    $ aisloader -bucket=my_ais_bucket -duration=10s -pctput=100 -provider=ais -readertype=tar -etl=tar2tf -cleanup=false
    ```

**18**. Timed 100% GET _directly_ from S3 bucket (notice '-s3endpoint' command line):
    ```console
    $ aisloader -bucket=s3://xyz -cleanup=false -numworkers=8 -pctput=0 -duration=10m -s3endpoint=https://s3.amazonaws.com
    ```

**19**. PUT approx. 8000 files into s3 bucket directly, skip printing usage and defaults. Similar to the previous example, aisloader goes directly to a given S3 endpoint ('-s3endpoint'), and aistore is not being used:
    ```console
     $ aisloader -bucket=s3://xyz -cleanup=false -minsize=16B -maxsize=16B -numworkers=8 -pctput=100 -totalputsize=128k -s3endpoint=https://s3.amazonaws.com -quiet
    ```

**20**.  Generate a list of object names (once), and then run aisloader without executing list-objects:

    ```console
    $ ais ls ais://nnn --props name -H > /tmp/a.txt
    $ aisloader -bucket=ais://nnn -duration 1h -numworkers=30 -pctput=0 -filelist /tmp/a.txt -cleanup=false
    ```

## Collecting stats

Collecting is easy - `aisloader` supports at-runtime monitoring via with Graphite using StatsD.
When starting up, `aisloader` will try to connect
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

## HTTP tracing

Following is a brief illustrated sequence to enable detailed tracing, capture statistics, and **toggle** tracing on/off at runtime.

**IMPORTANT NOTE:**
> The amount of generated (and extremely detailed) metrics can put a strain on your StatsD server. That's exactly the reason for runtime switch to **toggle** HTTP tracing on/off. The example below shows how to do it (in particular, see `kill -HUP`).

### 1. Run aisloader for 90s (32 workes, 100% write, sizes between 1KB and 1MB) with detailed tracing enabled:

```console
$ aisloader -bucket=ais://abc -duration 90s -numworkers=32 -minsize=1K -maxsize=1M -pctput=50 --cleanup=false --trace-http=true
```

### 2. Have `netcat` listening on the default StatsD port `8125`:

```console
$ nc 8125 -l -u -k

# The result will look as follows - notice "*latency*" metrics (in milliseconds):

...
aisloader.u18044-0.put.latency.posthttp:0.0005412320409368235|ms|@0.000049
aisloader.u18044-0.put.latency.proxyheader:0.06676835268647904|ms|@0.000049
aisloader.u18044-0.put.latency.targetresponse:0.7371088368431411|ms|@0.000049aisproxy.DLEp8080.put.count:587262|caistarget.vuIt8081.kalive.ms:1|ms
aistarget.vuIt8081.disk.sda.avg.rsize:58982|g
aistarget.vuIt8081.disk.sda.avg.wsize:506227|g
aistarget.vuIt8081.put.count:587893|c
aistarget.vuIt8081.put.redir.ms:2|ms
aistarget.vuIt8081.disk.sda.read.mbps:5.32|g
aistarget.vuIt8081.disk.sda.util:3|gaisloader.u18044-0.get.count:0|caisloader.u18044-0.put.count:19339|caisloader.u18044-0.put.pending:7|g|@0.000052aisloader.u18044-0.put.latency:4.068928072806246|ms|@0.000052
aisloader.u18044-0.put.minlatency:0|ms|@0.000052
aisloader.u18044-0.put.maxlatency:60|ms|@0.000052aisloader.u18044-0.put.throughput:1980758|g|@0.000052aisloader.u18044-0.put.latency.posthttp:0.0005170898185014737|ms|@0.000052
aisloader.u18044-0.put.latency.proxyheader:0.06742851233259217|ms|@0.000052
aisloader.u18044-0.put.latency.proxyrequest:0.1034696726821449|ms|@0.000052
aisloader.u18044-0.put.latency.targetheader:0.0699622524432494|ms|@0.000052
aisloader.u18044-0.put.latency.targetrequest:0.09168002482031129|ms|@0.000052
aisloader.u18044-0.put.latency.targetresponse:0.806660116862299|ms|@0.000052
aisloader.u18044-0.put.latency.proxy:0.6616681317544858|ms|@0.000052
aisloader.u18044-0.put.latency.targetconn:1.0948859816950205|ms|@0.000052
aisloader.u18044-0.put.latency.proxyresponse:0.425616629608563|ms|@0.000052
aisloader.u18044-0.put.latency.proxyconn:1.2669734732923108|ms|@0.000052
aisloader.u18044-0.put.latency.target:1.0063602047675682|ms|@0.000052aisproxy.DLEp8080.put.count:605044|caistarget.vuIt8081.put.redir.ms:2|ms
...
```

### 3. Finally, toggle detailed tracing on and off by sending aisoader `SIGHUP`:

```console
$ pgrep -a aisloader
3800 aisloader -bucket=ais://abc -duration 90s -numworkers=32 -minsize=1K -maxsize=1M -pctput=100 --cleanup=false --trace-http=true

# kill -1 3800
# or, same: kill -HUP 3800
```

### The result:

```console
Time      OP    Count                   Size (Total)            Latency (min, avg, max)                 Throughput (Avg)        Errors (Total)
10:11:27  PUT   20,136 (20,136 8 0)     19.7MiB (19.7MiB)       755.308µs  3.929ms    42.493ms         1.97MiB/s (1.97MiB/s)   -
...
Detailed latency info is disabled
...

# As stated, `SIGHUP` is a binary toggle - next time used it'll enable detailed trace with `aisloader printing:

Detailed latency info is enabled
```

> Note that other than `--trace-http`, all command-line options in this section are used for purely illustrative purposes.

# AISLoader Composer

For benchmarking production-level clusters, a single AISLoader instance may not be able to fully saturate the load the cluster can handle. In this case, multiple aisloader instances can be coordinated via the [AISLoader Composer](/bench/tools/aisloader-composer/). See the [README](/bench/tools/aisloader-composer/README.md) for instructions on setting up. 


## References

For documented `aisloader` metrics, please refer to:

* [aisloader metrics](/docs/metrics.md#ais-loader-metrics)

The same readme (above) also describes:

* [Statistics, Collected Metrics, Visualization](/docs/metrics.md)

For [StatsD](https://github.com/etsy/statsd) compliant backends, see:

* [StatsD backends](https://github.com/statsd/statsd/blob/master/docs/backend.md#supported-backends)

Finally, for another supported - and alternative to StatsD - monitoring via Prometheus integration, see:

* [Prometheus](/docs/prometheus.md)
