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
   regex           Regular expression to select table columns (use (?i) for case-insensitive matching), e.g.:
                    --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                    --regex "(?i)put|err" - match PUT and error columns regardless of case;
                    --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                    'ais performance counters --regex "AWS-GET|VERSION-CHANGE"'  - show AWS backend reads and out-of-band version changes;
                    'ais performance counters --regex "gcp-get|version-change"'  - show Google Cloud backend reads and out-of-band version changes;
   units           Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                   iec - IEC format, e.g.: KiB, MiB, GiB (default)
                   si  - SI (metric) format, e.g.: KB, MB, GB
                   raw - do not convert to (or from) human-readable format
   verbose,v       Verbose output
   help, h         Show help

```

The backend-read and out-of-band version-change examples use `ais performance counters`; version-change counters are not available in the throughput or disk views.

> Note that `ais show performance` and its shorter versioni, `ais performance`, can be used interchangeably.

## `ais performance throughput`

Example usage:

```console
$ ais performance throughput --refresh 30
```

## `ais show performance latency`

Example usage:

```console
$ ais show performance latency --refresh 10 --regex "AWS-GET|GET"
```

The following columns illustrate a subset of possible matches; they are not captured from cluster output or benchmark measurements. The broad `GET` pattern can select other GET-related columns too:

```text
TARGET          AWS-GET(n)  AWS-GET(t)  GET(n)  GET(t)
t[example]      ...         ...         ...     ...
```

`(n)` denotes a count. For each target, `(t)` is computed from changes in cumulative nanoseconds and counts only when both endpoint counters are present and the count increases by at least 4. With 1–3 requests, the displayed latency can be `0`; that does not mean service latency was zero. Without `--regex`, columns with zero activity can be omitted. With an explicit `--regex`, registered matching columns are included even if their values are zero; a regex cannot add an unregistered metric.

| Metric | Meaning |
| ------ | ------- |
| `GET(n)` | Warm and cold object GETs; excludes internal `GetObjReader` calls (for example, from copy or transform jobs), but counts successful GFN peer HTTP GETs through the normal GET response path. |
| `AWS-GET(n)` | S3-backend reads, including copy or transform, prefetch, and blob downloads; not a user-only cold GET counter. Internal name: `aws.get.n`. |
| `AWS-GET(t)` | Interval latency derived from `aws.get.ns.total` and `aws.get.n`, including backend work to store, copy, or transform objects. |
| `GET(t)` | Object GET latency, including remote work on cold GETs. |

The former cold-GET-specific count, remote-read/write latency, and size columns are no longer reported. Backend reads and ordinary GETs count different operations; do not subtract one from the other to infer cold user GETs.

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
   regex           Regular expression to select table columns (use (?i) for case-insensitive matching), e.g.:
                    --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                    --regex "(?i)put|err" - match PUT and error columns regardless of case;
                    --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                    'ais performance counters --regex "AWS-GET|VERSION-CHANGE"'  - show AWS backend reads and out-of-band version changes;
                    'ais performance counters --regex "gcp-get|version-change"'  - show Google Cloud backend reads and out-of-band version changes;
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
   regex         Regular expression to select table columns (use (?i) for case-insensitive matching), e.g.:
                  --regex "put|err" - show PUT (count), PUT (total size), and all supported error counters;
                  --regex "(?i)put|err" - match PUT and error columns regardless of case;
                  --regex "[a-z]" - show all supported metrics, including those that have zero values across all nodes;
                  'ais performance counters --regex "AWS-GET|VERSION-CHANGE"'  - show AWS backend reads and out-of-band version changes;
                  'ais performance counters --regex "gcp-get|version-change"'  - show Google Cloud backend reads and out-of-band version changes;
   summary       Tally up target disks to show per-target read/write summary stats and average utilizations
   units         Show statistics and/or parse command-line specified sizes using one of the following units of measurement:
                 iec - IEC format, e.g.: KiB, MiB, GiB (default)
                 si  - SI (metric) format, e.g.: KB, MB, GB
                 raw - do not convert to (or from) human-readable format
   help, h       Show help

```
