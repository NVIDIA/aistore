# AIS Loader
`aisloader` is a command-line tool that is included with AIS and that can be immediately used to generate load and evaluate cluster performance.

## Setup
Run `go build` to create an executable for `aisloader` or run

```
$ go run main.go worker.go [ARGS ...]
```

## Bytes Multiplicative Suffix

Parameters in AISLoader that represent a number of bytes can be specified with a multiplicative suffix. For example: `8M` would specify 8 MiB. The following metric prefix symbols are supported: 't' or 'T' - TiB 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. Note that this is entirely optional, and therefore an input such as `300` will be interpreted as 300 Bytes. 

## Using AIS Loader

AIS Loader allows for configurable PUT and GET tests directly from the command line
 - `-ip` -  IP address for proxy server
 - `-port` - Port number for proxy server
 - `-statsinterval` - Interval to show stats in seconds; 0 = disabled
 - `-bucket` - Bucket name
 - `-bckprovider` - "local" for local bucket, "cloud" for cloud bucket
 - `-duration` - How long to run the test; 0 = Unbounded
 - `-numworkers` - Number of go routines sending requests in parallel
 - `-pctput` - Percentage of put request (0% - 100%, remaining is allocated for GET)
 - `-tmpdir` - Local temporary directory used to store temporary files
 - `-totalputsize` - Stops after total put size exceeds this, can specify with [multiplicative suffix](#bytes-multiplicative-suffix), 0 = no limit
 - `-cleanup` - Determines if aisloader cleans up the files it creates when run is finished, true by default.
 - `-verifyhash` - If set, the contents of the downloaded files are verified using the xxhash in response headers during GET requests.
 - `-minsize` - Minimal object size, can specify with [multiplicative suffix](#bytes-multiplicative-suffix)
 - `-maxsize` - Maximal object size, can specify with [multiplicative suffix](#bytes-multiplicative-suffix)
 - `-readertype` - Type of reader: sg (default) | file | inmem | rand
 - `-loaderid` - ID to identify a loader when multiple instances of loader running on the same host
 - `-check-statsd` - If set, checks if statsd is running before run
 - `-statsdip` - IP for statsd server
 - `-statsdport` - UDP port number for statsd server
 - `-batchsize` - List and delete batch size
 - `-getconfig` - If set, aisloader tests reading the configuration of the proxy instead of the usual GET/PUT requests. 
 - `-seed` - Seed for random source, 0=use current time

### Examples

```sh
$ ./aisloader -bucket=my_local_bucket -duration=10s -pctput=50 -bckprovider=local -cleanup=true -readertype=sg -numworkers=3
```

This command will perform a performance test consisting of 50% PUT and 50% GET requests. It should return the following

```
Found 0 existing objects
Run configuration:
{
    "proxy": "http://172.50.0.2:8080",
    "local": true,
    "bucket": "my_local_bucket",
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

***Warning:*** Performance tests generate a heavy load on your local system, please save your work.

## Dry-Run Performance Tests

AIStore support two variations of "dry" deployment: AIS_NODISKIO and AIS_NOTNETIO.

Example of deploying a cluster with disk IO disabled and object size 256KB:

```bash
/aistore/ais$ AIS_NODISKIO=true AIS_DRYOBJSIZE=256k make deploy
```

**Note:** These are passed in either as environment variables or command line arguments when deploying the AIStore clusters.

| CLI Argument | Environment Variable | Default Value | Behavior |
| ------------ | ------ | ------ | ------------- |
| nodiskio | AIS_NODISKIO | `false` | If `true` - disables disk IO. For GET requests, a storage target does not read anything from disks - no file stat, file open etc - and returns an in-memory object with predefined size (see AIS_DRYOBJSIZE variable). For PUT requests, it reads the request's body to `/dev/null`. <br> Valid values are `true` or `1`, and `false` or `0`. |
| nonetio | AIS_NOTNETIO | `false` | If `true` - disables HTTP read and write. For GET requests, a storage target reads the data from disks but does not send bytes to a caller. It results in that the caller always gets an empty object. For PUT requests, after opening a connection, AIStore reads the data from in-memory object and saves the data to disks. <br> Valid values are `true` or `1`, and `false` or `0`. |
| dryobjsize | AIS_DRYOBJSIZE | 8m | A size of an object when a source is a 'fake' one: disk IO disabled for GET requests, and network IO disabled for PUT requests. The size is in bytes but suffixes can be used. The following suffixes are supported: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. <br> Default value is '8m' - the size of an object is 8 megabytes |

***Warning:*** The command-line load generator shows 0 bytes throughput for GET operations when network IO is disabled because a caller opens a connection but a storage target does not write anything to it. In this case the throughput can be calculated only indirectly by comparing total number of GETs or latency of the current test and those of previous test that had network IO enabled.

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

We provide simple [script](/ais/setup/deploy_grafana.sh) which allows you to set
up the Graphite and Grafana servers which run inside separate dockers. To add
new dashboards and panels, please follow: [grafana tutorial](http://docs.grafana.org/guides/getting_started/).

When selecting series in panel view, it should be in format: `stats.aisloader.<loader>.*`.
Remember that metrics will not be visible (and you will not be able to select
them) until you start the loader.
