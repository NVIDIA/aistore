`ais performance` or (same) `ais show performance` command supports the following 5 (five) subcommands:

```console
$ ais performance <TAB-TAB>

counters     throughput   latency      capacity     disk
```

Further, use `--help` to display any of the five performance subcommands, e.g.:

```console
$ ais performance throughput --help

NAME:
   ais performance throughput - Show GET and PUT throughput, associated (cumulative, average) sizes and counters

USAGE:
   ais performance throughput [TARGET_ID] [command options]

OPTIONS:
   --average-size       Show average GET, PUT, etc. request size
   --count value        Used together with '--refresh' to limit the number of generated reports, e.g.:
                         '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --no-headers, -H     Display tables without headers
   --non-verbose, --nv  Non-verbose (quiet) output, minimized reporting, fewer warnings
   --refresh value      Time interval for continuous monitoring; can be also used to update progress bar (at a given interval);
                        valid time units: ns, us (or µs), ms, s (default), m, h
   --regex value        Regular expression to select table columns (case-insensitive), e.g.:
                         --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                         --regex "Put|ERR" - same as above;
                         --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                         --regex "(AWS-GET$|VERSION-CHANGE$)" - show the number object version changes (updates) and cold GETs from AWS
                         --regex "(gcp-get$|version-change$)" - same as above for Google Cloud ('gs://')
   --units value        Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                        iec - IEC format, e.g.: KiB, MiB, GiB (default)
                        si  - SI (metric) format, e.g.: KB, MB, GB
                        raw - do not convert to (or from) human-readable format
   --verbose, -v        Verbose output
   --help, -h           Show help
```

> Note that `ais show performance` and its shorter versioni, `ais performance`, can be used interchangeably.

## `ais performance throughput`

Example usage:

```console
$ ais performance throughput --refresh 30
```

## `ais show performance latency`

Example usage:

```console
$ ais show performance latency --refresh 10

latency ------------------ 13:03:58.329680
TARGET           GET-COLD(n)     GET-COLD-RW(t)  GET-COLD(total/avg size)       GET(n)  GET(t)  GET-REDIR(t)    GET(total/avg size)
t[EkMt8081]      151             2.01s           145.00MiB  983.31KiB           154     2.13s   1.156551ms      154.00MiB  1.00MiB

latency ------------------ 13:04:08.335764
TARGET           GET-COLD(n)     GET-COLD-RW(t)  GET-COLD(total/avg size)       GET(n)  GET(t)  GET-REDIR(t)    GET(total/avg size)
t[EkMt8081]      189             2.04s           181.00MiB  980.66KiB           190     1.86s   892.015µs       190.00MiB  1.00MiB
```

Notice naming conventions:

* (n) - counter (total number of operations of a given kind)
* (t) - time (latency of the operation)

Other notable semantics includes:

| metric | comment |
| ------ | ------- |
| `GET-COLD-RW(t)` | denotes (remote read, local write) latency, which is a _part_ of the total latency  _not_ including the time it takes to transmit requested payload to user |
| `GET(t)` | GET latency (for cold GETs includes the above) |
| `GET-REDIR(t)` | time that passes between ais gateway _redirecting_ GET operation to specific target, and this target _starting_ to handle the request |

## `ais show performance counters`

```console
$ ais performance counters --help
NAME:
   ais performance counters - show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
        - numbers of list-objects requests;
        - (GET, PUT, etc.) cumulative and average sizes;
        - associated error counters, if any.

USAGE:
   ais show performance counters [TARGET_ID] [command options]

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports, e.g.:
                      '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --no-headers, -H  display tables without headers
   --regex value     regular expression select table columns (case-insensitive), e.g.:
                      --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                      --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                      --regex "(GET-COLD$|VERSION-CHANGE$)" - show the number of cold GETs and object version changes (updates)
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --average-size    show average GET, PUT, etc. request size
```

## `ais performance disk`

```console
$ ais performance disk --help
NAME:
   ais show performance disk - show disk utilization and read/write statistics

USAGE:
   ais show performance disk [TARGET_ID] [command options]

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports, e.g.:
                      '--refresh 10 --count 5' - run 5 times with 10s interval (default: 0)
   --no-headers, -H  display tables without headers
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --regex value     regular expression select table columns (case-insensitive), e.g.:
                      --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                      --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                      --regex "(GET-COLD$|VERSION-CHANGE$)" - show the number of cold GETs and object version changes (updates)
   --summary         tally up target disks to show per-target read/write summary stats and average utilizations
```
