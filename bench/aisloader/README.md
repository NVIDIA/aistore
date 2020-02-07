`aisloader` is a command-line tool that is included with AIS and that can be immediately used to generate load and evaluate cluster performance.

## Setup

To build `aisloader`, run `make aisloader` from the root of the aistore repo:

```console
$ cd $GOPATH/src/github.com/NVIDIA/aistore
$ make aisloader
```

## Bytes Multiplicative Suffix

Parameters in AISLoader that represent the number of bytes can be specified with a multiplicative suffix. For example: `8M` would specify 8 MiB. The following multiplicative suffixes are supported: 't' or 'T' - TiB 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. Note that this is entirely optional, and therefore an input such as `300` will be interpreted as 300 Bytes.

## Using AIS Loader

AIS Loader allows for configurable PUT and GET tests directly from the command line
 - `-ip` -  IP address for proxy server
 - `-port` - Port number for proxy server
 - `-statsinterval` - Interval to show stats / send statsd in seconds; 0 = disabled
 - `-bucket` - Bucket name
 - `-provider` - "ais" for AIS bucket, "cloud" for Cloud bucket; other supported values include "gcp" and "aws", for Amazon and Google clouds, respectively
 - `-duration` - How long to run the test; 0 = Unbounded
 - `-numworkers` - Number of go routines sending requests in parallel
 - `-pctput` - Percentage of put request (0% - 100%, remaining is allocated for GET)
 - `-tmpdir` - Local temporary directory used to store temporary files
 - `-totalputsize` - Stops after total put size exceeds this, may contain [multiplicative suffix](#bytes-multiplicative-suffix), 0 = no limit
 - `-cleanup` - Determines if aisloader cleans up the files it creates when run is finished, true by default.
 - `-verifyhash` - If set, the contents of the downloaded files are verified using the xxhash in response headers during GET requests.
 - `-minsize` - Minimal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix)
 - `-maxsize` - Maximal object size, may contain [multiplicative suffix](#bytes-multiplicative-suffix)
 - `-maxputs` - Maximum number of objects to PUT
 - `-readertype` - Type of reader: sg (default) | file | inmem | rand
 - `-loaderid` - ID to identify a loader when multiple instances of loader running on the same host
 - `-check-statsd` - If set, checks if statsd is running before run
 - `-statsdip` - IP for statsd server
 - `-statsdport` - UDP port number for statsd server
 - `-batchsize` - List and delete batch size
 - `-getconfig` - If set, aisloader tests reading the configuration of the proxy instead of the usual GET/PUT requests.
 - `-seed` - Seed for random source, 0=use current time
 - `-readoff` - Read range offset, may contain [multiplicative suffix](#bytes-multiplicative-suffix)
 - `-readlen` - Read range length(0 - GET full object), may contain [multiplicative suffix](#bytes-multiplicative-suffix)
 - `-loadernum` - Total number of aisloaders running concurrently and generating combined load. If defined, must be greater than the loaderid and cannot be used together with loaderidhashlen
 - `-getloaderid` - Print out stored/computed unique loaderID(aisloader identifier) and exit
 - `-loaderidhashlen` - Size (in bits) of the generated aisloader identifier. Cannot be used together with loadernum
 - `-randomname` - Generate objects names of 32 random characters. This option is ignored when loadernum is defined
 - `-subdir` - Virtual destination directory for all aisloader-generated objects. If not set, all generated objects are PUT into bucket's root
 - `-putshards` - Spread generated objects over this many subdirectories (max 100k)
 - `-uniquegets` - If set, GET objects randomly and equally(it makes sure *not* to GET some objects more frequently than the others). If not set, GET selects a random object every time, that may result, e.g, in reading the same object twice in a row
 - `-no-detailed-stats` - If set, disable collecting detailed HTTP latencies for PUT and GET (to minimize amount of stats sent to statsD)
 - `-dry-run` - Show the configuration and parameters that aisloader will use for benchmark and exit

### Examples

The following performs a 10-seconds performance test of 50% PUT and 50% GET requests:

```console
$ aisloader -bucket=my_ais_bucket -duration=10s -pctput=50 -provider=ais -cleanup=true -readertype=sg -numworkers=3
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
    "# workers": 3,
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

Loader ID:

```console
$ aisloader -getloaderid
0x0
$ aisloader -getloaderid -loaderid=loaderstring -loaderidhashlen=8
0xdb
```

PUT 2000 objects with names that look like hex({0..2000}{loaderid}):

```console
$ aisloader -bucket=nvais -duration 10s -numworkers=3 -loaderid=11 -loadernum=20 -maxputs=2000
```

**Warning:** Performance tests generate a heavy load on your local system, please save your work.

## Dry-Run Performance Tests

AIStore supports a "dry" deployment: AIS_NODISKIO.

Example of deploying a cluster with disk IO disabled and object size 256KB:

```console
$ AIS_NODISKIO=true AIS_DRYOBJSIZE=256k make deploy
```

**Note:** These are passed in either as environment variables or command line arguments when deploying the AIStore clusters.

| CLI Argument | Environment Variable | Default Value | Behavior |
| ------------ | ------ | ------ | ------------- |
| nodiskio | AIS_NODISKIO | `false` | If `true` - disables disk IO. For GET requests, a storage target does not read anything from disks - no file stat, file open etc - and returns an in-memory object with predefined size (see AIS_DRYOBJSIZE variable). For PUT requests, it reads the request's body to `/dev/null`. <br> Valid values are `true` or `1`, and `false` or `0`. |
| dryobjsize | AIS_DRYOBJSIZE | 8m | A size of an object when a source is a 'fake' one: disk IO disabled for GET requests, and network IO disabled for PUT requests. The size is in bytes but suffixes can be used. The following suffixes are supported: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. <br> Default value is '8m' - the size of an object is 8 megabytes |

**Warning:** The command-line load generator shows 0 bytes throughput for GET operations when network IO is disabled because a caller opens a connection but a storage target does not write anything to it. In this case, the throughput can be calculated only indirectly by comparing total number of GETs or latency of the current test and those of the previous test that had network IO enabled.

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
