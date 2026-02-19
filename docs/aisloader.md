# AIS Loader (`aisloader`)

AIS Loader ([`aisloader`](/bench/tools/aisloader)) is a tool to measure storage performance. It is a load generator that we constantly use to benchmark and stress-test [AIStore](https://github.com/NVIDIA/aistore) or any S3-compatible backend.

In fact, aisloader can list, write, and read S3(**) buckets _directly_, which makes it quite useful, convenient, and easy to use benchmark to compare storage performance **with** AIStore in front of S3 and **without**.

> (**) `aisloader` can be further easily extended to work directly with any Cloud storage provider including, but not limited to, AIStore-supported GCP, OCI, and Azure.

In addition, `aisloader` generates synthetic workloads that mimic training and inference workloads - the capability that allows to run benchmarks in isolation (which is often preferable) avoiding compute-side bottlenecks (if any) and associated complexity.

There's a large set of command-line switches that allow to realize almost any conceivable workload, with basic permutations always including:

* number of workers
* read and write sizes
* read and write ratios

Detailed protocol-level tracing statistics are also available - see [HTTP tracing](#http-tracing) section below for brief introduction.

---

**October 2025 update:** aisloader retains its StatsD integration for operational and benchmarking use cases. While AIStore itself now uses Prometheus exclusively, aisloader (for now) continues to emit its runtime metrics via StatsD — allowing DevOps to collect and visualize performance data from large aisloader fleets.

To integrate aisloader with Prometheus-based observability stacks, run the official Prometheus StatsD Exporter (which would translate aisloader’s StatsD metrics into Prometheus format).

---

**Table of Contents**

- [Setup](#setup)
- [Command Line Options](#command-line-options)
  - [Quick Reference (Alphabetical)](#quick-reference-alphabetical)
  - [Command Line Options Grouped by Category](#command-line-options-grouped-by-category)
- [Assorted Command Line](#assorted-command-line)
- [Environment variables](#environment-variables)
- [Archive Workload](#archive-workload)
- [Multipart Download Stream](#multipart-download-stream)
- [Get-Batch Support](#get-batch-support)
- [Random Access Across Very Large Collections](#random-access-across-very-large-collections)
- [Examples](#examples)
- [Collecting stats](#collecting-stats)
  - [Grafana](#grafana)
- [HTTP tracing](#http-tracing)
- [AISLoader-Composer](#aisloader-composer)
- [References](#references)

## Setup

You can install `aisloader` using the following Bash script:

* https://github.com/NVIDIA/aistore/blob/main/scripts/install_from_binaries.sh

```console
./scripts/install_from_binaries.sh --help
```

Alternatively, you can also build the tool directly from the source:

```console
## uncomment if needed:
## git clone https://github.com/NVIDIA/aistore.git
## cd aistore
##
$ make aisloader
```

For usage, run: `aisloader`, `aisloader usage`, or `aisloader --help`.

For usage examples and extended commentary, see also:

* https://github.com/NVIDIA/aistore/blob/main/bench/tools/aisloader/test/ci-test.sh

## Command Line Options

This section presents two alternative, intentionally redundant views for usability: a concise alphabetical quick reference for fast lookups, and a grouped-by-category presentation with explanations and examples for deeper understanding.

For the most recently updated command-line options and examples, please run `aisloader` or `aisloader usage`.

### Quick Reference (Alphabetical)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -arch.format | `string` | Archive format (`.tar`, `.tgz`, `.tar.gz`, `.zip`, `.tar.lz4`) | `.tar` |
| -arch.minsize | `string` | Minimum size of files inside shards, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -arch.maxsize | `string` | Maximum size of files inside shards, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -arch.num-files | `int` | Number of archived files per shard (PUT only; 0 = auto-computed from file sizes) | `0` |
| -arch.pct | `int` | Percentage of PUTs that create shards (0-100); does NOT affect GET operations | `0` |
| -arch.prefix | `string` | Optional prefix inside archive (e.g., `trunk-` or `a/b/c/trunk-`) | `""` |
| -bprops | `json` | JSON string formatted as per the SetBucketProps API and containing bucket properties to apply | `""` |
| -bucket | `string` | Bucket name or bucket URI. If empty, aisloader generates a new random bucket name | `""` |
| -cached | `bool` | List in-cluster objects - only those objects from a remote bucket that are present ("cached") | `false` |
| -cksum-type | `string` | Checksum type to use for PUT object requests | `xxhash` |
| -cleanup | `bool` | When true, remove bucket upon benchmark termination (must be specified for AIStore buckets) | `n/a` (required) |
| -cont-on-err | `bool` | GetBatch: ignore missing files and/or objects - include them under `__404__/` prefix and keep going | `false` |
| -dry-run | `bool` | Show the entire set of parameters that aisloader will use when actually running | `false` |
| -duration | `duration` | Benchmark duration (0 - run forever or until Ctrl-C). If not specified and totalputsize > 0, runs until totalputsize reached | `1m` |
| -epochs | `int` | Number of "epochs" to run whereby each epoch entails full pass through the entire listed bucket | `0` |
| -etl | `string` | Built-in ETL, one of: `tar2tf`, `md5`, or `echo`. Each object that aisloader GETs undergoes the selected transformation | `""` |
| -etl-spec | `string` | Custom ETL specification (pathname). Must be compatible with Kubernetes Pod specification | `""` |
| -evict-batchsize | `int` | Batch size to list and evict the next batch of remote objects | `1000` |
| -filelist | `string` | Local or locally accessible text file containing object names (for subsequent reading) | `""` |
| -get-batchsize | `int` | Use GetBatch API (ML endpoint) instead of GetObject | `0` |
| -getloaderid | `bool` | When true, print stored/computed unique loaderID and exit | `false` |
| -ip | `string` | AIS proxy/gateway IP address or hostname | `localhost` |
| -json | `bool` | When true, print the output in JSON | `false` |
| -latest | `bool` | When true, check in-cluster metadata and possibly GET the latest object version from the associated remote bucket | `false` |
| -list-dirs | `bool` | List virtual subdirectories (remote buckets only) | `false` |
| -loaderid | `string` | ID to identify a loader among multiple concurrent instances | `0` |
| -loaderidhashlen | `int` | Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum | `0` |
| -loadernum | `int` | Total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen | `0` |
| -maxputs | `int` | Maximum number of objects to PUT | `0` |
| -maxsize | `string` | Maximal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1GiB` |
| -minsize | `string` | Minimal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1MiB` |
| -mpdstream-chunk-size | `int` | Chunk size for multipart download stream, may contain [multiplicative suffix](#bytes-multiplicative-suffix) (0 = API default: 8MiB) | `0` |
| -mpdstream-workers | `int` | Number of concurrent workers for multipart download stream (0 = API default: 16) | `0` |
| -multipart-chunks | `int` | Number of chunks for multipart upload (0 = disabled, >0 = use multipart with specified chunks) | `0` |
| -num-subdirs | `int` | Spread generated objects over this many virtual subdirectories (< 100k) | `0` |
| -numworkers | `int` | Number of goroutine workers operating on AIS in parallel | `10` |
| -pctmpdstream | `int` | Percentage of GET operations that use multipart download stream (0-100) | `0` |
| -pctmultipart | `int` | Percentage of PUT operations that use multipart upload (0-100, only applies when multipart-chunks > 0) | `0` |
| -pctput | `int` | Percentage of PUTs in the aisloader-generated workload (see also: `-arch.pct`) | `0` |
| -pctupdate | `int` | Percentage of GET requests that are followed by a PUT "update" (i.e., creation of a new version of the object) | `0` |
| -perm-shuffle-max | `int` | Max names for shuffle-based name-getter (above this uses O(1) memory affine) | `100000` |
| -port | `string` | AIS proxy/gateway port | `8080` |
| -provider | `string` | `ais` for AIS bucket, `aws`, `azure`, `gcp`, `oci` for Amazon, Azure, Google, and Oracle clouds respectively | `ais` |
| -putshards | `int` | **Deprecated** - use `-num-subdirs` instead | `0` |
| -quiet | `bool` | When starting to run, do not print command line arguments, default settings, and usage examples | `false` |
| -randomname | `bool` | When true, generate object names of 32 random characters. This option is ignored when loadernum is defined | `true` |
| -randomproxy | `bool` | When true, select random gateway ("proxy") to execute each I/O request | `false` |
| -readertype | `string` | Type of reader: `sg` (default), `file`, `rand`, `tar` | `sg` |
| -readlen | `string` | Read range length, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -readoff | `string` | Read range offset, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -s3endpoint | `string` | S3 endpoint to read/write S3 bucket directly (with no AIStore) | `""` |
| -s3profile | `string` | Other than default S3 config profile referencing alternative credentials | `""` |
| -s3-use-path-style | `bool` | Use older path-style addressing (e.g., `https://s3.amazonaws.com/BUCKET/KEY`). Should only be used with `-s3endpoint` | `false` |
| -seed | `int` | Random seed to achieve deterministic reproducible results (0 = use current time in nanoseconds) | `0` |
| -skiplist | `bool` | When true, skip listing objects in a bucket before running 100% PUT workload | `false` |
| -stats-output | `string` | Filename to log statistics (empty string = standard output) | `""` |
| -statsdip | `string` | **Deprecated** - StatsD IP address or hostname | `localhost` |
| -statsdport | `int` | **Deprecated** - StatsD UDP port | `8125` |
| -statsdprobe | `bool` | **Deprecated** - Test-probe StatsD server prior to benchmarks | `false` |
| -statsinterval | `int` | Interval in seconds to print performance counters (0 = disabled) | `10` |
| -stoppable | `bool` | When true, allow termination via Ctrl-C | `false` |
| -subdir | `string` | For GET: prefix that may or may not be an actual [virtual directory](/docs/howto_virt_dirs.md). For PUT: virtual destination directory for all generated objects. See [CLI `--prefix`](/docs/cli/object.md) | `""` |
| -timeout | `duration` | Client HTTP timeout (0 = infinity) | `10m` |
| -tmpdir | `string` | Local directory to store temporary files | `/tmp/ais` |
| -tokenfile | `string` | Authentication token (FQN) | `""` |
| -totalputsize | `string` | Stop PUT workload once cumulative PUT size reaches or exceeds this value, can contain [multiplicative suffix](#bytes-multiplicative-suffix) (0 = no limit) | `0` |
| -trace-http | `bool` | Trace HTTP latencies (see [HTTP tracing](#http-tracing)) | `false` |
| -uniquegets | `bool` | When true, GET objects randomly and equally (i.e., avoid getting some objects more frequently than others) | `true` |
| -usage | `bool` | Show command-line options, usage, and examples | `false` |
| -verifyhash | `bool` | Checksum-validate GET: recompute object checksums and validate against the one received with GET metadata | `false` |

### Command Line Options Grouped by Category

#### Cluster Connection and API Configuration (`clusterParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -ip | `string` | AIS proxy/gateway IP address or hostname | `localhost` |
| -port | `string` | AIS proxy/gateway port | `8080` |
| -randomproxy | `bool` | When true, select random gateway ("proxy") to execute each I/O request | `false` |
| -timeout | `duration` | Client HTTP timeout (0 = infinity) | `10m` |
| -tokenfile | `string` | Authentication token (FQN) | `""` |
| -s3endpoint | `string` | S3 endpoint to read/write S3 bucket directly (with no AIStore) | `""` |
| -s3profile | `string` | Other than default S3 config profile referencing alternative credentials | `""` |
| -s3-use-path-style | `bool` | Use older path-style addressing (e.g., `https://s3.amazonaws.com/BUCKET/KEY`). Should only be used with `-s3endpoint` | `false` |

#### Target Bucket and Properties (`bucketParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -bucket | `string` | Bucket name or bucket URI. If empty, aisloader generates a new random bucket name | `""` |
| -provider | `string` | `ais` for AIS bucket, `aws`, `azure`, `gcp`, `oci` for Amazon, Azure, Google, and Oracle clouds respectively | `ais` |
| -bprops | `json` | JSON string formatted as per the SetBucketProps API and containing bucket properties to apply | `""` |

#### Timing, Intensity, and Name-Getter Configuration (`workloadParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -duration | `duration` | Benchmark duration (0 - run forever or until Ctrl-C). If not specified and totalputsize > 0, runs until totalputsize reached | `1m` |
| -numworkers | `int` | Number of goroutine workers operating on AIS in parallel | `10` |
| -pctput | `int` | Percentage of PUTs in the aisloader-generated workload (see also: `-arch.pct`) | `0` |
| -pctupdate | `int` | Percentage of GET requests that are followed by a PUT "update" (i.e., creation of a new version of the object) | `0` |
| -epochs | `int` | Number of "epochs" to run whereby each epoch entails full pass through the entire listed bucket | `0` |
| -perm-shuffle-max | `int` | Max names for shuffle-based name-getter (above this uses O(1) memory affine) | `100000` |
| -seed | `int` | Random seed to achieve deterministic reproducible results (0 = use current time in nanoseconds) | `0` |
| -maxputs | `int` | Maximum number of objects to PUT | `0` |
| -totalputsize | `string` | Stop PUT workload once cumulative PUT size reaches or exceeds this value, can contain [multiplicative suffix](#bytes-multiplicative-suffix) (0 = no limit) | `0` |
| -skiplist | `bool` | When true, skip listing objects in a bucket before running 100% PUT workload | `false` |
| -uniquegets | `bool` | When true, GET objects randomly and equally (i.e., avoid getting some objects more frequently than others) | `true` |

#### Object Size Constraints and Integrity (`sizeCksumParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -minsize | `string` | Minimal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1MiB` |
| -maxsize | `string` | Maximal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix) | `1GiB` |
| -cksum-type | `string` | Checksum type to use for PUT object requests | `xxhash` |
| -verifyhash | `bool` | Checksum-validate GET: recompute object checksums and validate against the one received with GET metadata | `false` |
| -readertype | `string` | Type of reader: `sg` (default), `file`, `rand`, `tar` | `sg` |
| -tmpdir | `string` | Local directory to store temporary files | `/tmp/ais` |

#### Archive/Shard Configuration (`archParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -arch.format | `string` | Archive format (`.tar`, `.tgz`, `.tar.gz`, `.zip`, `.tar.lz4`) | `.tar` |
| -arch.prefix | `string` | Optional prefix inside archive (e.g., `trunk-` or `a/b/c/trunk-`) | `""` |
| -arch.num-files | `int` | Number of archived files per shard (PUT only; 0 = auto-computed from file sizes) | `0` |
| -arch.minsize | `string` | Minimum size of files inside shards, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -arch.maxsize | `string` | Maximum size of files inside shards, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -arch.pct | `int` | Percentage of PUTs that create shards (0-100); does NOT affect GET operations | `0` |

#### Object Naming Strategy (`namingParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -num-subdirs | `int` | Spread generated objects over this many virtual subdirectories (< 100k) | `0` |
| -filelist | `string` | Local or locally accessible text file containing object names (for subsequent reading) | `""` |
| -list-dirs | `bool` | List virtual subdirectories (remote buckets only) | `false` |
| -randomname | `bool` | When true, generate object names of 32 random characters. This option is ignored when loadernum is defined | `true` |
| -subdir | `string` | For GET: prefix that may or may not be an actual [virtual directory](/docs/howto_virt_dirs.md). For PUT: virtual destination directory for all generated objects. See [CLI `--prefix`](/docs/cli/object.md) | `""` |
| -putshards | `int` | **Deprecated** - use `-num-subdirs` instead | `0` |

#### Read Operation Configuration (`readParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -readoff | `string` | Read range offset, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -readlen | `string` | Read range length, can contain [multiplicative suffix](#bytes-multiplicative-suffix) | `""` |
| -get-batchsize | `int` | Use GetBatch API (ML endpoint) instead of GetObject | `0` |
| -latest | `bool` | When true, check in-cluster metadata and possibly GET the latest object version from the associated remote bucket | `false` |
| -cached | `bool` | List in-cluster objects - only those objects from a remote bucket that are present ("cached") | `false` |
| -evict-batchsize | `int` | Batch size to list and evict the next batch of remote objects | `1000` |
| -cont-on-err | `bool` | GetBatch: ignore missing files and/or objects - include them under `__404__/` prefix and keep going | `false` |

#### Multipart Upload Settings (`multipartParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -multipart-chunks | `int` | Number of chunks for multipart upload (0 = disabled, >0 = use multipart with specified chunks) | `0` |
| -pctmultipart | `int` | Percentage of PUT operations that use multipart upload (0-100, only applies when multipart-chunks > 0) | `0` |

#### Multipart Download Stream Settings (`mpdStreamParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -mpdstream-chunk-size | `int64` | Chunk size for multipart download stream, can contain [multiplicative suffix](#bytes-multiplicative-suffix) (0 = API default: 8MiB) | `0` |
| -mpdstream-workers | `int` | Number of concurrent workers for multipart download stream (0 = API default: 16) | `0` |
| -pctmpdstream | `int` | Percentage of GET operations that use multipart download stream (0-100) | `0` |

#### ETL Configuration (`etlParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -etl | `string` | Built-in ETL, one of: `tar2tf`, `md5`, or `echo`. Each object that aisloader GETs undergoes the selected transformation | `""` |
| -etl-spec | `string` | Custom ETL specification (pathname). Must be compatible with Kubernetes Pod specification | `""` |

#### Fleet Coordination (`loaderParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -loaderid | `string` | ID to identify a loader among multiple concurrent instances | `0` |
| -loadernum | `int` | Total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen | `0` |
| -loaderidhashlen | `int` | Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum | `0` |
| -getloaderid | `bool` | When true, print stored/computed unique loaderID and exit | `false` |

#### Statistics and Monitoring (`statsParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -stats-output | `string` | Filename to log statistics (empty string = standard output) | `""` |
| -statsinterval | `int` | Interval in seconds to print performance counters (0 = disabled) | `10` |
| -json | `bool` | When true, print the output in JSON | `false` |
| -statsdip | `string` | **Deprecated** - StatsD IP address or hostname | `localhost` |
| -statsdport | `int` | **Deprecated** - StatsD UDP port | `8125` |
| -statsdprobe | `bool` | **Deprecated** - Test-probe StatsD server prior to benchmarks | `false` |

#### Cleanup, Dry-Run, HTTP Tracing, Termination Control (`miscParams`)

| Command-line option | Type | Description | Default |
| --- | --- | --- | --- |
| -cleanup | `bool` | When true, remove bucket upon benchmark termination (must be specified for AIStore buckets) | `n/a` (required) |
| -dry-run | `bool` | Show the entire set of parameters that aisloader will use when actually running | `false` |
| -trace-http | `bool` | Trace HTTP latencies (see [HTTP tracing](#http-tracing)) | `false` |
| -stoppable | `bool` | When true, allow termination via Ctrl-C | `false` |
| -quiet | `bool` | When starting to run, do not print command line arguments, default settings, and usage examples | `false` |
| -usage | `bool` | Show command-line options, usage, and examples | `false` |

## Assorted Command Line

### Duration

The loads can run for a given period of time (option `-duration <duration>`) or until the specified amount of data is generated (option `-totalputsize=<total size in KBs>`).

If both options are provided the test finishes on the whatever-comes-first basis.

Example 100% write into the bucket "abc" for 2 hours:

```console
$ aisloader -bucket=abc -provider=ais -duration 2h -totalputsize=4000000 -pctput=100
```

The above will run for two hours or until it writes around 4GB data into the bucket, whatever comes first.

### Write vs Read

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

### Read range

The loader can read the entire object (default) **or** a range of object bytes.

To set the offset and length to read, use option `-readoff=<read offset (in bytes)>` and `readlen=<length to read (in bytes)>`.

For convenience, both options support size suffixes: `k` - for KiB, `m` - for MiB, and `g` - for GiB.

Example that reads a 32MiB segment at 1KB offset from each object stored in the bucket "abc":

```console
$ aisloader -bucket=ais://abc -duration 5m -cleanup=false -readoff=1024 -readlen=32m
```

The test (above) will run for 5 minutes and will not "cleanup" after itself (next section).

### Cleanup

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

### Object size

For the PUT workload the loader generates randomly-filled objects. But what about object sizing?

By default, object sizes are randomly selected as well in the range between 1MiB and 1GiB. To set preferred (or fixed) object size(s), use the options `-minsize=<minimal object size in KiB>` and `-maxsize=<maximum object size in KiB>`

### Setting bucket properties

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

---

## Environment variables

| Environment Variable | Type | Description |
| -- | -- | -- |
| `AIS_ENDPOINT` | `string` | Cluster's endpoint: http or https address of any AIStore gateway in [this cluster](/docs/overview.md#at-a-glance). Overrides `ip` and `port` flags. |

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

---

## Archive Workload

AIStore supports packing many small files into *shards* (TAR, ZIP, TGZ, LZ4-TAR) to improve performance and reduce metadata overhead.

AISLoader can benchmark both **archive creation (PUT)** and **reading individual files from existing shards (GET)**.

### Archive Parameters

| Parameter | Description |
|----------|-------------|
| `-arch.pct` | Percentage of PUTs that create shards (0–100). Does **not** affect GET operations. `100` = all PUTs create shards; `30` = 30% shards, 70% plain objects. |
| `-arch.format` | Archive format: `.tar` (default), `.tgz`, `.tar.gz`, `.zip`, `.tar.lz4`. |
| `-arch.num-files` | Files per shard for PUT. `0` = auto-computed from `arch.minsize` / `arch.maxsize`. |
| `-arch.minsize` | Minimum size of files inside shards (supports multiplicative suffixes). |
| `-arch.maxsize` | Maximum size of files inside shards (supports multiplicative suffixes). |

When the bucket contains shards, aisloader automatically:
1. Lists objects with archive expansion enabled
2. Detects archived files (e.g., `shard-987.tar/file-042.bin`)
3. Reads from them using the `?archpath=` API parameter

The displayed statistics will show whether objects are plain or archived, e.g.:

```console
Found 108,959 plain objects and 1,089,590 archived files (91% archived)
```

### Usage Examples

#### **Create 100% shards (each containing 10 files)**

```console
$ aisloader -bucket=ais://abc -pctput=100 -arch.pct=100 \
            -arch.num-files=10 -arch.minsize=1K -arch.maxsize=100K \
            -duration=5m -cleanup=false
```

All PUTs create shards; each shard contains 10 files between 1KB and 100KB.

#### **Mixed workload: 30% shards, 70% plain objects**

```console
$ aisloader -bucket=ais://abc -pctput=100 -arch.pct=30 \
            -arch.num-files=10 -arch.minsize=1K -arch.maxsize=100K \
            -duration=5m -cleanup=false
```

30% of PUT operations create shards; the rest create plain objects.

#### **Read from existing shards**

```console
$ aisloader -bucket=ais://abc -pctput=0 -duration=1h -cleanup=false
```

When the target bucket contains shards, aisloader automatically:

1. Lists objects with archive expansion enabled
2. Identifies archived files (e.g., `photos.tar/00042.jpg`)
3. Issues GET requests using `?archpath=` to retrieve individual files inside shards

Example startup message:

```console
Found 108,959 plain objects and 1,089,590 archived files (91% archived)
```

#### **Create shards using a specific archive format**

```console
$ aisloader -bucket=ais://abc -pctput=100 -arch.pct=100 -arch.format=.tgz -arch.num-files=20 -duration=5m -cleanup=false
```

### Performance Considerations

* **Small-file workloads benefit greatly** from sharding: fewer large objects → fewer metadata lookups → higher throughput.
* **Archived GETs add CPU overhead**, especially for compressed formats (`.tgz`, `.tar.lz4`).
* **Throughput vs. operation rate tradeoff**:

  * Large plain objects → high MB/s
  * Many tiny files inside shards → lower MB/s but similar (or higher) operations/sec

Example typical comparison:

* Reading 16KB plain objects: **~250 MiB/s**
* Reading 1KB archived files: **~13 MiB/s**, but with comparable GET operations/sec

### Limitations

* **Multipart uploads** of shards are not yet supported (requires streaming chunk writer).
* **Direct S3 access** (`-s3endpoint`) does not support archive operations (sharding requires AIStore).

For more information about AIStore’s archive/shard support, see
* [AIStore Archive Documentation](/docs/archive.md).

---

## Multipart Download Stream

`aisloader` can benchmark the `MultipartDownloadStream` API, which downloads a single object using multiple concurrent HTTP range requests. This improves single-object GET throughput for [chunked](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0#chunked-objects) objects by engaging multiple disks on the server side.

### Configuration

| Flag | Description |
|------|-------------|
| `-pctmpdstream` | Percentage of GETs that use multipart download stream (0-100) |
| `-mpdstream-workers` | Number of concurrent download workers per object (0 = API default: 16) |
| `-mpdstream-chunk-size` | Size of each range request (0 = API default: 8MiB) |

When `-pctmpdstream` is non-zero, the specified fraction of GET operations will use `MultipartDownloadStream` instead of regular single-stream GET. The remaining GETs use the standard path. Statistics for MPD stream operations are tracked separately (labeled `GET-MPDSTREAM` in the output).

### Usage Example

100% read using multipart download stream with 32 workers:

```console
$ aisloader -bucket=ais://my_bucket -pctput=0 -duration=10m -numworkers=4 \
            -pctmpdstream=100 -mpdstream-workers=32 -mpdstream-chunk-size=64MiB \
            -cleanup=false
```

Mixed workload — 50% regular GET, 50% MPD stream:

```console
$ aisloader -bucket=ais://my_bucket -pctput=0 -duration=10m -numworkers=8 \
            -pctmpdstream=50 -mpdstream-workers=16 -cleanup=false
```

### Restrictions

* Cannot be combined with `-s3endpoint` (direct S3 access)
* Cannot be combined with `-get-batchsize`
* Cannot be combined with `-readoff` / `-readlen` (range reads)

---

## Get-Batch Support

With version 2.1, `aisloader` can now benchmark [Get-Batch](#getbatch-distributed-multi-object-retrieval) operations using the `--get-batchsize` flag (range: 1-1000). The tool consumes TAR streams (see note below), validates archived file counts, and tracks Get-Batch-specific statistics. The `--continue-on-err` flag enables testing of soft-error handling behavior.

> Supported serialization formats include: `.tar` (default), `.tar.gz`, `.tar.lz4`, and `.zip`.

## Random Access Across Very Large Collections

The tool uses the [`name-getter`] abstraction (see https://github.com/NVIDIA/aistore/blob/main/bench/tools/aisloader/namegetter/ng.go) to enable efficient random reads across very large collections: objects and archived files.

The `--epochs N` flag enables full-dataset read passes, with different algorithms selected automatically based on dataset size:

**PermAffinePrime**: For datasets larger than `100k` (by default) objects, an affine transformation with prime modulus provides memory-efficient pseudo-random access without storing full permutations. The algorithm fills batch requests completely and may span epoch boundaries.

**PermShuffle**: For datasets up to (default) `100k` objects, Fisher-Yates shuffle with uint32 indices (50% memory reduction compared to previous implementation).

**Selection Logic:**

| Workload | Dataset Size | Selected Algorithm |
|---|---:|---|
| Mixed read/write or non-epoched workloads | any | Random / RandomUnique |
| Read-only | <= `100k` objects (default) | PermShuffle |
| Read-only | >  `100k` objects (--/--) | PermAffinePrime |

> Command-line override to set the size threshold (instead of default `100k`): `--perm-shuffle-max` flag.

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
    $ aisloader -bucket=ais://my_bucket -duration=10s -pctput=100 -arch.pct=100 -arch.num-files=10 -arch.minsize=1K -arch.maxsize=10K -cleanup=false
    ```

**17**. Generate load on `tar2tf` ETL. New ETL is started and then stopped at the end. TAR files are PUT to the cluster. Only available when cluster is deployed on Kubernetes.

    ```console
    $ aisloader -bucket=my_ais_bucket -duration=10s -pctput=100 -provider=ais -readertype=tar -etl=tar2tf -cleanup=false
    ```

**18**. Timed 100% GET _directly_ from S3 bucket (notice '-s3endpoint' command line):
    ```console
    $ aisloader -bucket=s3://xyz -cleanup=false -numworkers=8 -pctput=0 -duration=10m -s3endpoint=https://s3.amazonaws.com
    ```

**19**. PUT approx. 8000 files into s3 bucket directly, skip printing usage and defaults. Similar to the previous example, aisloader goes directly to a given S3 endpoint ('-s3endpoint'), and AIStore is not being used:
    ```console
     $ aisloader -bucket=s3://xyz -cleanup=false -minsize=16B -maxsize=16B -numworkers=8 -pctput=100 -totalputsize=128k -s3endpoint=https://s3.amazonaws.com -quiet
    ```

**20**.  Generate a list of object names (once), and then run aisloader without executing list-objects:

    ```console
    $ ais ls ais://nnn --props name -H > /tmp/a.txt
    $ aisloader -bucket=ais://nnn -duration 1h -numworkers=30 -pctput=0 -filelist /tmp/a.txt -cleanup=false
    ```

**21**. GetBatch example: read random batches each consisting of 64 archived files

```console
    $ ais ls ais://nnn --summary
    NAME             PRESENT         OBJECTS         SIZE (apparent, objects, remote)        USAGE(%)
    ais://nnn        yes             108959 0        1.67GiB 1.66GiB 0B                      0%

    $ aisloader -bucket=ais://nnn -pctput=0 -duration=90m -numworkers=4 -cleanup=false -get-batchsize=64 --quiet -epochs 7 -cont-on-err
    Found 1,089,590 archived files

    Runtime configuration:
    {
       "proxy": "http://ais-endpoint:51080",
       "bucket": "ais://nnn",
       "duration": "1h30m0s",
       "# workers": 4,
       "stats interval": "10s",
       "GET(batch): batch size": 64,
       "archive (shards)": {
          "% workload": 100,
          "format": ".tar",
          "minimum file size": 1024,
          "maximum file size": 1048576
       },
       "name-getter": "unique epoch-based",
       "cleanup": false
    }

    Time      OP    Count                   Size (Total)            Latency (min, avg, max)                 Throughput (Avg)        Errors (Total)
    14:16:45  GBT   6,602 (6,602)           412.6MiB (412.6MiB)     4.399ms    6.030ms    22.278ms          41.26MiB/s (41.26MiB/s) -
    14:16:55  GBT   6,397 (12,999)          399.8MiB (812.4MiB)     4.566ms    6.225ms    17.136ms          39.98MiB/s (40.62MiB/s) -
    14:17:05  GBT   6,201 (19,200)          387.6MiB (1.2GiB)       4.609ms    6.424ms    22.505ms          38.75MiB/s (40.00MiB/s) -
    14:17:15  GBT   6,127 (25,327)          382.9MiB (1.5GiB)       4.821ms    6.500ms    21.599ms          38.30MiB/s (39.57MiB/s) -
    14:17:25  GBT   6,153 (31,480)          384.6MiB (1.9GiB)       4.765ms    6.473ms    23.135ms          38.46MiB/s (39.35MiB/s) -
    ...
```

---

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
* `metric` - can be: `latency.*`, `get.*`, `put.*`

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

### 1. Run aisloader for 90s (32 workers, 100% write, sizes between 1KB and 1MB) with detailed tracing enabled:

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

For AIS observability (including CLI, Prometheus, and Kubernetes integration), please see:

* [AIS Observability Overview](/docs/monitoring-overview.md)

For [StatsD](https://github.com/etsy/statsd) compliant backends, see:

* [StatsD backends](https://github.com/statsd/statsd/blob/master/docs/backend.md#supported-backends)

Finally, for another supported - and alternative to StatsD - monitoring via Prometheus integration, see:

* [Prometheus](/docs/monitoring-prometheus.md)
