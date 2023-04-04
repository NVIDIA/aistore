---
layout: post
title: CLUSTER
permalink: /docs/cli/cluster
redirect_from:
 - /cli/cluster.md/
 - /docs/cli/cluster.md/
---

# Introduction

The `ais cluster` command supports the following subcommands:

```console
$ ais cluster <TAB-TAB>
show               remote-detach      set-primary        decommission       reset-stats
remote-attach      rebalance          shutdown           add-remove-nodes
```

Alternatively, use `--help` to show subcommands with brief descriptions:

```console
$ ais cluster --help
NAME:
   ais cluster - monitor and manage AIS cluster: add/remove nodes, change primary gateway, etc.

USAGE:
   ais cluster command [command options] [arguments...]

COMMANDS:
   show              show cluster nodes and utilization
   remote-attach     attach remote ais cluster
   remote-detach     detach remote ais cluster
   rebalance
   set-primary       select a new primary proxy/gateway
   shutdown          shutdown cluster
   decommission      decommission entire cluster
   add-remove-nodes  manage cluster membership (scale up or down)
   reset-stats       reset cluster or node stats (all cumulative metrics or only errors)

OPTIONS:
   --help, -h  show help

```

As always, each subcommand will have its own help and usage examples (the latter possibly spread across multiple documents).

> For any keyword or text of any kind, you can easily look up examples and descriptions (if available) via a simple `find`, for instance:

```console
$ find . -type f -name "*.md" | xargs grep "ais.*mountpath"
```

Note that there is a single CLI command to [grow](#join-a-node) a cluster, and multiple commands to scale it down.

Scaling down can be done gracefully or forcefully, and also temporarily or permanently.

For background, usage examples, and details, please see [this document](/docs/leave_cluster.md).

# CLI Reference for Cluster and Node (Daemon) management
This section lists cluster and node management operations within the AIS CLI, via `ais cluster`.

## Table of Contents
- [Cluster and Node status](#cluster-and-node-status)
- [Show cluster map](#show-cluster-map)
- [Show cluster stats](#show-cluster-stats)
- [Show disk stats](#show-disk-stats)
- [Join a node](#join-a-node)
- [Remove a node](#remove-a-node)
- [Remote AIS cluster](#remote-ais-cluster)
  - [Attach remote cluster](#attach-remote-cluster)
  - [Detach remote cluster](#detach-remote-cluster)
  - [Show remote clusters](#show-remote-clusters)

## Cluster and Node status

The command has a rather long(ish) short description and multiple subcommands:

```console
$ ais show cluster --help
NAME:
   ais show cluster - show cluster nodes and utilization

USAGE:
   ais show cluster command [command options] [NODE_ID] | [target [NODE_ID]] | [proxy [NODE_ID]] |
                       [smap [NODE_ID]] | [bmd [NODE_ID]] | [config [NODE_ID]] | [stats [NODE_ID]]

COMMANDS:
   smap    show Smap (cluster map)
   bmd     show BMD (bucket metadata)
   config  show cluster and node configuration
   stats   (alias for "ais show performance") show performance counters, throughput, latency, and more (press <TAB-TAB> to select specific view)

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --json, -j        json input/output
   --no-headers, -H  display tables without headers
   --help, -h        show help
```

To quickly exemplify, let's assume the cluster has a (target) node called `t[xyz]`. Then:


### show cluster: all nodes (including t[xyz]) and gateways, as well as deployed version and runtime stats
```console
$ ais show cluster
```

### show all target (nodes) and, again, runtime statistics, software version, deployment type, K8s pods, and more
```console
$ ais show cluster target
```

### show specific target
```console
$ ais show cluster target t[xyz]
```

### ask specific target to show its cluster map
```console
$ ais show cluster smap t[xyz]
```

and so on and so forth.

### Notes

> The last example (above) may potentially make sense when troubleshooting. Otherwise, by design and implementation, cluster map (`Smap`), bucket metadata (`BMD`), and all other cluster-level metadata exists in identical protected and versioned replicas on all nodes at any given point in time.

> Still, to display cluster map in its (JSON) fullness, run:

```console
$ ais show cluster smap --json
```

> `--json` option is almost universally supported in CLI

> Similar to all other `show` commands, `ais cluster show` is an alias for `ais cluster show`. Both can be used interchangeably.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Can be used in combination with `--refresh` option to limit the number of generated reports | `1` |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds) | ` ` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

### Examples

```console
$ ais show cluster
PROXY            MEM USED %      MEM AVAIL       UPTIME
pufGp8080[P]     0.28%           15.43GiB        17m
ETURp8083        0.26%           15.43GiB        17m
sgahp8082        0.26%           15.43GiB        17m
WEQRp8084        0.27%           15.43GiB        17m
Watdp8081        0.26%           15.43GiB        17m

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME
iPbHt8088        0.28%           15.43GiB        14.00%          1.178TiB        0.13%           -               17m
Zgmlt8085        0.28%           15.43GiB        14.00%          1.178TiB        0.13%           -               17m
oQZCt8089        0.28%           15.43GiB        14.00%          1.178TiB        0.14%           -               17m
dIzMt8086        0.28%           15.43GiB        14.00%          1.178TiB        0.13%           -               17m
YodGt8087        0.28%           15.43GiB        14.00%          1.178TiB        0.14%           -               17m

Summary:
 Proxies:       5 (0 - unelectable)
 Targets:       5
 Primary Proxy: pufGp8080
 Smap Version:  14
 Deployment:    dev
```

## Show cluster map

`ais show cluster smap [NODE_ID]`

Show a copy of the cluster map (Smap) stored on `NODE_ID`.

If `NODE_ID` is not given, show cluster map from (primary or secondary) proxy "pointed to" by your local CLI configuration (`ais config cli`) or `AIS_ENDPOINT` environment.

> Note that cluster map (`Smap`), bucket metadata (`BMD`), and all other cluster-level metadata exists in identical protected and versioned replicas on all nodes at any given point in time.

Useful variations include `ais show cluster smap --json` (to see the unabridged version), and also:

```console
$ ais show cluster smap --refresh 5
```

The latter will periodically (until Ctrl-C) show cluster map in 5-second intervals - might be useful in presence of any kind of membership changes (e.g., cluster startup).

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--count` | `int` | Can be used in combination with `--refresh` option to limit the number of generated reports | `1` |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds) | ` ` |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show smap from a given node

Ask a specific node for its cluster map (Smap) replica:

```console
$ ais show cluster smap <TAB-TAB>
... p[ETURp8083] ...

$ ais show cluster smap p[ETURp8083]
NODE             TYPE    PUBLIC URL
ETURp8083        proxy   http://127.0.0.1:8083
WEQRp8084        proxy   http://127.0.0.1:8084
Watdp8081        proxy   http://127.0.0.1:8081
pufGp8080[P]     proxy   http://127.0.0.1:8080
sgahp8082        proxy   http://127.0.0.1:8082

NODE             TYPE    PUBLIC URL
YodGt8087        target  http://127.0.0.1:8087
Zgmlt8085        target  http://127.0.0.1:8085
dIzMt8086        target  http://127.0.0.1:8086
iPbHt8088        target  http://127.0.0.1:8088
oQZCt8089        target  http://127.0.0.1:8089

Non-Electable:

Primary Proxy: pufGp8080
Proxies: 5       Targets: 5      Smap Version: 14
```

## Show cluster stats

`ais show cluster stats` is a alias for `ais show performance`.

The latter is the primary implementation, and the preferred way to investigate cluster performance, while `ais show cluster stats` is retained in part for convenience and in part for backward compatibility.

```console
$ ais show cluster stats <TAB-TAB>
counters     throughput   latency      capacity     disk

$ ais show cluster stats --help
NAME:
   ais show cluster stats - (alias for "ais show performance") show performance counters, throughput, latency, and more (press <TAB-TAB> to select specific view)

USAGE:
   ais show cluster stats command [command options] [TARGET_ID]

COMMANDS:
   counters    show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts, as well as:
               - numbers of list-objects requests;
               - (GET, PUT, etc.) cumulative and average sizes;
               - associated error counters, if any, and more.
   throughput  show GET and PUT throughput, associated (cumulative, average) sizes and counters
   latency     show GET, PUT, and APPEND latencies and average sizes
   capacity    show target mountpaths, disks, and used/available capacity
   disk        show disk utilization and read/write statistics

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --all             when printing tables, show all columns including those that have only zero values
   --no-headers, -H  display tables without headers
   --regex value     regular expression to select table columns (case-insensitive), e.g.: --regex "put|err"
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --average-size    show average GET, PUT, etc. request size
   --help, -h        show help
```

See also:

* [ais show performance`](/docs/cli/show.md) 

## Show disk stats

`ais show storage disk [TARGET_ID]` - show disk utilization and read/write statistics

```console
$ ais show storage disk --help
NAME:
   ais show storage disk - show disk utilization and read/write statistics

USAGE:
   ais show storage disk [command options] [TARGET_ID]

OPTIONS:
   --refresh value   interval for continuous monitoring;
                     valid time units: ns, us (or µs), ms, s (default), m, h
   --count value     used together with '--refresh' to limit the number of generated reports (default: 0)
   --no-headers, -H  display tables without headers
   --units value     show statistics and/or parse command-line specified sizes using one of the following _units of measurement_:
                     iec - IEC format, e.g.: KiB, MiB, GiB (default)
                     si  - SI (metric) format, e.g.: KB, MB, GB
                     raw - do not convert to (or from) human-readable format
   --regex value     regular expression to select table columns (case-insensitive), e.g.: --regex "put|err"
   --summary         tally up target disks to show per-target read/write summary stats and average utilizations
   --help, -h        show help
```

When `TARGET_ID` is not given, disk stats for all targets will be shown and aggregated.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Can be used in combination with `--refresh` option to limit the number of generated reports | `1` |
| `--refresh` | `duration` | Refresh interval - time duration between reports. The usual unit suffixes are supported and include `m` (for minutes), `s` (seconds), `ms` (milliseconds) | ` ` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

### Examples

#### Display disk reports stats N times every M seconds

Display 5 reports of all targets' disk statistics, with 10s intervals between each report.

```console
$ ais show storage disk --count 2 --refresh 10s
Target		Disk	Read		Write		%Util
163171t8088	sda	6.00KiB/s	171.00KiB/s	49
948212t8089	sda	6.00KiB/s	171.00KiB/s	49
41981t8085	sda	6.00KiB/s	171.00KiB/s	49
490062t8086	sda	6.00KiB/s	171.00KiB/s	49
164472t8087	sda	6.00KiB/s	171.00KiB/s	49

Target		Disk	Read		Write		%Util
163171t8088	sda	1.00KiB/s	4.26MiB/s	96
41981t8085	sda	1.00KiB/s	4.26MiB/s	96
948212t8089	sda	1.00KiB/s	4.26MiB/s	96
490062t8086	sda	1.00KiB/s	4.29MiB/s	96
164472t8087	sda	1.00KiB/s	4.26MiB/s	96
```

## Join a node

`ais cluster add-remove-nodes join --role=proxy IP:PORT`

Join a proxy to the cluster.

`ais cluster add-remove-nodes join --role=target IP:PORT`

Join a target to the cluster.

Note: The node will try to join the cluster using an ID it detects (either in the filesystem's xattrs or on disk) or that it generates for itself.
If you would like to specify an ID, you can do so while starting the [`aisnode` executable](/docs/command_line.md).

### Examples

#### Join node

Join a proxy node with socket address `192.168.0.185:8086`

```console
$ ais cluster add-remove-nodes join --role=proxy 192.168.0.185:8086
Proxy with ID "23kfa10f" successfully joined the cluster.
```

## Remove a node

**Temporarily remove an existing node from the cluster:**

`ais cluster add-remove-nodes start-maintenance NODE_ID`
`ais cluster add-remove-nodes stop-maintenance NODE_ID`

Starting maintenance puts the node in maintenance mode, and the cluster gradually transitions to
operating without the specified node (which is labeled `maintenance` in the cluster map). Stopping
maintenance will revert this.

`ais cluster add-remove-nodes shutdown NODE_ID`

Shutting down a node will put the node in maintenance mode first, and then shut down the `aisnode`
process on the node.


**Permanently remove an existing node from the cluster:**

`ais cluster add-remove-nodes decommission NODE_ID`

Decommissioning a node will safely remove a node from the cluster by triggering a cluster-wide
rebalance first. This can be avoided by specifying `--no-rebalance`.


### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--no-rebalance` | `bool` | By default, `ais cluster add-remove-nodes maintenance` and `ais cluster add-remove-nodes decommission` triggers a global cluster-wide rebalance. The `--no-rebalance` flag disables automatic rebalance thus providing for the administrative option to rebalance the cluster manually at a later time. BEWARE: advanced usage only! | `false` |

### Examples

#### Decommission node

**Permananently remove proxy p[omWp8083] from the cluster:**

```console
$ ais cluster add-remove-nodes decommission <TAB-TAB>
p[cFOp8082]   p[Hqhp8085]   p[omWp8083]   t[bFat8087]   t[Icjt8089]   t[ofPt8091]
p[dpKp8084]   p[NGVp8081]   p[Uerp8080]   t[erbt8086]   t[IDDt8090]   t[TKSt8088]

$ ais cluster add-remove-nodes decommission p[omWp8083]

Node "omWp8083" has been successfully removed from the cluster.
```

**To terminate `aisnode` on a given machine, use the `shutdown` command, e.g.:**

```console
$ ais cluster add-remove-nodes shutdown t[23kfa10f]
```

Similar to the `maintenance` option, `shutdown` triggers global rebalancing then shuts down the corresponding `aisnode` process (target `t[23kfa10f]` in the example above).

#### Temporarily put node in maintenance

```console
$ ais show cluster
PROXY            MEM USED %      MEM AVAIL       UPTIME
202446p8082      0.09%           31.28GiB        70s
279128p8080[P]   0.11%           31.28GiB        80s

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME
147665t8084      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               70s
165274t8087      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               70s

$ ais cluster add-remove-nodes start-maintenance 147665t8084
$ ais show cluster
PROXY            MEM USED %      MEM AVAIL       UPTIME
202446p8082      0.09%           31.28GiB        70s
279128p8080[P]   0.11%           31.28GiB        80s

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME  STATUS
147665t8084      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               71s     maintenance
165274t8087      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               71s     online
```

#### Take a node out of maintenance

```console
$ ais cluster add-remove-nodes stop-maintenance t[147665t8084]
$ ais show cluster
PROXY            MEM USED %      MEM AVAIL       UPTIME
202446p8082      0.09%           31.28GiB        80s
279128p8080[P]   0.11%           31.28GiB        90s

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME
147665t8084      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               80s
165274t8087      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               80s
```

## Remote AIS cluster

Given an arbitrary pair of AIS clusters A and B, cluster B can be *attached* to cluster A, thus providing (to A) a fully-accessible (list-able, readable, writeable) *backend*.

For background, terminology, and definitions, and for many more usage examples, please see:

* [Remote AIS cluster](/docs/providers.md#remote-ais-cluster)
* [Usage examples and easy-to-use scripts for developers](/docs/development.md)

### Attach remote cluster

`ais cluster remote-attach UUID=URL [UUID=URL...]`

or

`ais cluster remote-attach ALIAS=URL [ALIAS=URL...]`

Attach a remote AIS cluster to a local one via the remote cluster public URL. Alias (a user-defined name) can be used instead of cluster UUID for convenience.
For more details and background on *remote clustering*, please refer to this [document](/docs/providers.md).

#### Examples

Attach two remote clusters, the first - by its UUID, the second one - via user-friendly alias (`two`).

```console
$ ais cluster remote-attach a345e890=http://one.remote:51080 two=http://two.remote:51080`
```

### Detach remote cluster

`ais cluster remote-detach UUID|ALIAS`

Detach a remote cluster using its alias or UUID.

#### Examples

Example below assumes that the remote has user-given alias `two`:

```console
$ ais cluster remote-detach two
```

### Show remote clusters

`ais show remote-cluster`

Show details about attached remote clusters.

#### Examples
The following two commands attach and then show the remote cluster at the address `my.remote.ais:51080`:

```console
$ ais cluster remote-attach alias111=http://my.remote.ais:51080
Remote cluster (alias111=http://my.remote.ais:51080) successfully attached
$ ais show remote-cluster
UUID      URL                     Alias     Primary         Smap  Targets  Online
eKyvPyHr  my.remote.ais:51080     alias111  p[80381p11080]  v27   10       yes
```

Notice that:

* user can assign an arbitrary name (aka alias) to a given remote cluster
* the remote cluster does *not* have to be online at attachment time; offline or currently unreachable clusters are shown as follows:

```console
$ ais show remote-cluster
UUID        URL                       Alias     Primary         Smap  Targets  Online
eKyvPyHr    my.remote.ais:51080       alias111  p[primary1]     v27   10       no
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```

Notice the difference between the first and the second lines in the printout above: while both clusters appear to be currently offline (see the rightmost column), the first one was accessible at some earlier time and therefore we show that it has (in this example) 10 storage nodes and other details.

To `detach` any of the previously configured associations, simply run:

```console
$ ais cluster remote-detach alias111
$ ais show remote-cluster
UUID        URL                       Alias     Primary         Smap  Targets  Online
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```
