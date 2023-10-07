---
layout: post
title: PERFORMANCE
permalink: /docs/cli/performance
redirect_from:
 - /cli/performance.md/
 - /docs/cli/performance.md/
---

`ais performance` or (same) `ais show performance` command supports the following 5 (five) subcommands:

```console
$ ais performance <TAB-TAB>
counters     throughput   latency      capacity     disk
```

## `ais show performance counters`

```console
$ ais show performance counters --help
NAME:
   ais show performance counters - show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
        - numbers of list-objects requests;
        - (GET, PUT, etc.) cumulative and average sizes;
        - associated error counters, if any, and more.

USAGE:
   ais show performance counters [command options] [TARGET_ID]

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

## `ais show performance disk`

```console
$ ais show performance disk --help
NAME:
   ais show performance disk - show disk utilization and read/write statistics

USAGE:
   ais show performance disk [command options] [TARGET_ID]

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
