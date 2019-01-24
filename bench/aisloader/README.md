# AIS Loader 
`aisloader` is a command-line tool that is included with AIS and that can be immediately used to generate load and evaluate cluster performance.

## Setup
Run `go build` to create an executable for `aisloader` or run

```
$ go run main.go worker.go [ARGS ...]
```
## Using AIS Loader

AIS Loader allows for configurable PUT and GET tests directly from the command line
 - `-ip` -  IP address for proxy server
 - `-port` - Port number for proxy server
 - `-statsinterval` - Interval to show stats in seconds; 0 = disabled
 - `-bucket` - Bucket name
 - `-local` - True if using local bucket 
 - `-duration` - How long to run the test; 0 = Unbounded
 - `-numworkers` - Number of go routines sending requests in parallel
 - `-pctput` - Percentage of put request (0% - 100%, remaining is allocated for GET)
 - `-tmpdir` - Local temporary directory used to store temporary files
 - `-totalputsize` - Stops after total put size exceeds this (in KB); 0 = no limit
 - `-cleanup` - True if clean up after run
 - `-verifyhash` - True if verify xxhash during get
 - `-minsize` - Minimal object size in KB
 - `-maxsize` - Maximal object size in KB
 - `-readertype` - Type of reader: sg (default) | file | inmem | rand
 - `-loaderid` - ID to identify a loader when multiple instances of loader running on the same host
 - `-statsdport` - UDP port number for local statsd server
 - `-batchsize` - List and delete batch size
 - `-getconfig` - True if send get proxy config requests only
 
### Examples

```sh
$ ./aisloader -bucket=my_local_bucket -duration=10s -pctput=50 -local=true -cleanup=true -readertype=sg -numworkers=3
```

This command will perform a performance test consisting of 50% PUT and 50% GET requests. It should return the following

>  Found 0 existing objects <br>
>  Run configuration:
```json
{
    "proxy": "http://172.50.0.2:8080",
    "local": true,
    "bucket": "my_local_bucket",
    "duration": "10s",
    "put upper bound": 0,
    "put %": 50,
    "minimal object size in KB": 1024,
    "maximal object size in KB": 1048576,
    "# workers": 3,
    "stats interval": "10s",
    "backed by": "sg",
    "cleanup": true
  }
  ```
  >Actual run duration: 10.313689487s

  | Time | OP | Count | Total Bytes | Latency(min, avg, max) |	Throughput | Error |
  | ---- | ---- | ---- | ---- | ---- | ---- | ---- |
  | 11:23:06 | Put | 18 | 7.35GB | 398.57ms &emsp; 1.55s &emsp; 3.40s | 664.83MB | 0 |
  | 11:23:06 | Get | 15 | 5.19GB | 85.10ms &emsp; 267.73ms &emsp; 586.19ms | 470.15MB | 0 |
  | 11:23:06 | CFG | 0  | 0B | 0.00ms &emsp; 0.00ms &emsp; 0.00ms | 0B | 0 |
>  11:23:06 Clean up ... <br>
>  11:23:06 Clean up done

***Warning:*** Performance tests generate a heavy load on your local system, please save your work.

## Dry-Run Performance Tests
AIStore support two variations of "dry" deployment: AIS_NODISKIO and AIS_NOTNETIO.

Example of deploying a cluster with disk IO disabled and object size 256KB:

```
/aistore/ais$ AIS_NODISKIO=true AIS_DRYOBJSIZE=256k make deploy
```

**Note:**These are passed in either as environment variables or command line arguments when deploying the AIStore clusters.

| CLI Argument | Environment Variable | Default Value | Behaviour |
| ------------ | ------ | ------ | ------------- |
| nodiskio | AIS_NODISKIO | `false` | If `true` - disables disk IO. For GET requests, a storage target does not read anything from disks - no file stat, file open etc - and returns an in-memory object with predefined size (see AIS_DRYOBJSIZE variable). For PUT requests, it reads the request's body to `/dev/null`. <br> Valid values are `true` or `1`, and `false` or `0`. |
| nonetio | AIS_NOTNETIO | `false` | If `true` - disables HTTP read and write. For GET requests, a storage target reads the data from disks but does not send bytes to a caller. It results in that the caller always gets an empty object. For PUT requests, after opening a connection, AIStore reads the data from in-memory object and saves the data to disks. <br> Valid values are `true` or `1`, and `false` or `0`. |
| dryobjsize | AIS_DRYOBJSIZE | 8m | A size of an object when a source is a 'fake' one: disk IO disabled for GET requests, and network IO disabled for PUT requests. The size is in bytes but suffixes can be used. The following suffixes are supported: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. <br> Default value is '8m' - the size of an object is 8 megabytes |

***Warning:*** The command-line load generator shows 0 bytes throughput for GET operations when network IO is disabled because a caller opens a connection but a storage target does not write anything to it. In this case the throughput can be calculated only indirectly by comparing total number of GETs or latency of the current test and those of previous test that had network IO enabled.
