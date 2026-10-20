`ais performance` or (same) `ais show performance` command supports six subcommands. The following command entries are excerpted from captured CLI help:

```text
   counters    Show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
   throughput  Show GET and PUT throughput, associated (cumulative, average) sizes and counters.
   latency     Show GET, PUT, and APPEND latencies and average sizes.
   capacity    Show target mountpaths, disks, and used/available capacity
   intra-data  Show intra-cluster streaming traffic: RX/TX object counts, sizes, throughput, and average size.
   disk        Show disk utilization and read/write statistics
```

Further, use `--help` to display any of the six performance subcommands, e.g.:

```console
$ ais performance throughput --help
NAME:
   ais performance throughput - Show GET and PUT throughput, associated (cumulative, average) sizes and counters.
     Examples:
     - 'ais performance throughput --refresh 10'            - update every 10s;
     - 'ais performance throughput --refresh 10 --count 5'  - 5 iterations, 10s apart.

USAGE:
   ais performance throughput [TARGET_ID] [command options]

OPTIONS:
   count           Used together with '--refresh' to limit the number of generated reports, e.g.:
                    '--refresh 10 --count 5' - run 5 times with 10s interval
   no-headers,H    Display tables without headers
   non-verbose,nv  Non-verbose (quiet) output, minimized reporting, fewer warnings
   refresh         Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                   valid time units: ns, us (or µs), ms, s (default), m, h
   regex           Regular expression to select table columns (case-insensitive), e.g.:
                    --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                    --regex "Put|ERR" - same as above;
                    --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                    --regex "aws-get|version-change" - show AWS backend reads and out-of-band object version changes;
                    --regex "gcp-get|version-change" - same as above for Google Cloud ('gs://')
   units           Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                   iec - IEC format, e.g.: KiB, MiB, GiB (default)
                   si  - SI (metric) format, e.g.: KB, MB, GB
                   raw - do not convert to (or from) human-readable format
   verbose,v       Verbose output
   help, h         Show help
```

> Note that `ais show performance` and its shorter version, `ais performance`, can be used interchangeably.

## `ais performance throughput`

Example usage:

```console
$ ais performance throughput --refresh 30
```

## `ais show performance latency`

Example usage:

```console
$ ais show performance latency --refresh 10 --regex get
```

Column naming:

* `(n)` - number of operations
* `(t)` - average latency over the refresh interval: change in cumulative time divided by change in count; shown as 0 when fewer than 4 requests complete during the interval

| Columns | Meaning |
| ------- | ------- |
| `GET(n)`, `GET(t)` | user GET requests, warm and cold; for cold GETs, latency includes reading from the remote backend; reads by copy/transform jobs are not included |
| `AWS-GET(n)`, `AWS-GET(t)` | reads from the AWS (S3) backend - by cold GETs and also by prefetch, blob download, and copy/transform jobs; latency includes storing (or copying/transforming) the object locally |

Other backends follow the same convention: `GCP-GET`, `AZURE-GET`, `OCI-GET`, and `REMAIS-GET` (remote AIS cluster).

Since backend reads include job traffic, the difference between `AWS-GET(n)` and `GET(n)` is not the number of cold GETs.

## `ais show performance counters`

```console
$ ais performance counters --help
NAME:
   ais performance counters - Show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
        - numbers of list-objects requests;
        - (GET, PUT, etc.) cumulative and average sizes;
        - associated error counters, if any, and more.

USAGE:
   ais performance counters [TARGET_ID] [command options]

OPTIONS:
   average-size    Show average GET, PUT, etc. request size
   count           Used together with '--refresh' to limit the number of generated reports, e.g.:
                    '--refresh 10 --count 5' - run 5 times with 10s interval
   no-headers,H    Display tables without headers
   non-verbose,nv  Non-verbose (quiet) output, minimized reporting, fewer warnings
   refresh         Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                   valid time units: ns, us (or µs), ms, s (default), m, h
   regex           Regular expression to select table columns (case-insensitive), e.g.:
                    --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                    --regex "Put|ERR" - same as above;
                    --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                    --regex "aws-get|version-change" - show AWS backend reads and out-of-band object version changes;
                    --regex "gcp-get|version-change" - same as above for Google Cloud ('gs://')
   units           Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                   iec - IEC format, e.g.: KiB, MiB, GiB (default)
                   si  - SI (metric) format, e.g.: KB, MB, GB
                   raw - do not convert to (or from) human-readable format
   verbose,v       Verbose output
   help, h         Show help
```

## `ais performance disk`

```console
$ ais performance disk --help
NAME:
   ais performance disk - Show disk utilization and read/write statistics

USAGE:
   ais performance disk [TARGET_ID] [command options]

OPTIONS:
   count         Used together with '--refresh' to limit the number of generated reports, e.g.:
                  '--refresh 10 --count 5' - run 5 times with 10s interval
   no-headers,H  Display tables without headers
   refresh       Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                 valid time units: ns, us (or µs), ms, s (default), m, h
   regex         Regular expression to select table columns (case-insensitive), e.g.:
                  --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                  --regex "Put|ERR" - same as above;
                  --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                  --regex "aws-get|version-change" - show AWS backend reads and out-of-band object version changes;
                  --regex "gcp-get|version-change" - same as above for Google Cloud ('gs://')
   summary       Tally up target disks to show per-target read/write summary stats and average utilizations
   units         Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                 iec - IEC format, e.g.: KiB, MiB, GiB (default)
                 si  - SI (metric) format, e.g.: KB, MB, GB
                 raw - do not convert to (or from) human-readable format
   help, h       Show help
```
