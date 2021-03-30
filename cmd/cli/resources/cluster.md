# CLI Reference for Cluster and Node (Daemon) management
This section lists cluster and node management operations the AIS CLI, with `ais cluster`.

## Table of Contents
- [Cluster or Daemon status](#cluster-or-daemon-status)
- [Show cluster map](#show-cluster-map)
- [Show disk stats](#show-disk-stats)
- [Join a node](#join-a-node)
- [Remove a node](#remove-a-node)
- [Attach remote cluster](#attach-remote-cluster)
- [Detach remote cluster](#detach-remote-cluster)
- [Show remote clusters](#show-remote-clusters)

## Cluster or Daemon status

`ais show cluster [DAEMON_TYPE|DAEMON_ID]`

Display information about `DAEMON_ID` or all nodes of `DAEMON_TYPE`. `DAEMON_TYPE` is either `proxy` or `target`.
If nothing is set, information from all the daemons in the AIS cluster is displayed.

> Note: Like many other `ais show` commands, `ais show cluster` is aliased to `ais cluster show` for ease of use.
> Both of these commands are used interchangeably throughout the documentation.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |
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

`ais show cluster smap [DAEMON_ID]`

Show a copy of the cluster map (smap) present on `DAEMON_ID`.
If `DAEMON_ID` is not set, it will show the smap of the daemon that the `AIS_ENDPOINT` points at.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show smap from a given node

Show smap copy of daemon with ID `ETURp8083`.

```console
$ ais show cluster smap ETURp8083
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

## Show disk stats

`ais show disk [TARGET_ID]`

Show the disk stats of the `TARGET_ID`. If `TARGET_ID` isn't given, disk stats for all targets will be shown.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

### Examples

#### Display disk reports stats N times every M seconds

Display 5 reports with disk statistics of all targets with 10s intervals between each report.

```console
$ ais show disk --count 2 --refresh 10s
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

`ais cluster membership join --type=proxy IP:PORT`

Join a proxy to the cluster.

`ais cluster membership join --type=target IP:PORT`

Join a target to the cluster.

Note: The node will try to join the cluster using an ID it detects (either in the filesystem's xattrs or on disk) or generates for itself.
If you would like to specify an ID, you can do so while starting the [`aisnode` executable](/docs/command_line.md).

### Examples

#### Join node

Join a proxy node with socket address `192.168.0.185:8086`

```console
$ ais cluster membership join --type=proxy 192.168.0.185:8086
Proxy with ID "23kfa10f" successfully joined the cluster.
```

## Remove a node

Temporarily remove an existing node from the cluster:

`ais cluster membership start-maintenance DAEMON_ID`
`ais cluster membership stop-maintenance DAEMON_ID`

Starting maintenance puts the node in maintenance mode, and the cluster gradually transitions to
operating without the specified node (which is labeled `maintenance` in the cluster map). Stopping
maintenance will revert this.

`ais cluster membership shutdown DAEMON_ID`

Shutting down a node will put the node in maintenance mode first, and then shut down the `aisnode`
process on the node.


Permanently remove an existing node from the cluster:

`ais cluster membership decommission DAEMON_ID`

Decommissioning a node will safely remove a node from the cluster by triggering a cluster-wide
rebalance first. This can be avoided by specifying `--no-rebalance`.


### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--no-rebalance` | `bool` | By default, `ais cluster membership maintenance` and `ais cluster membership decommission` triggers a global cluster-wide rebalance. The `--no-rebalance` flag disables automatic rebalance thus providing for the administrative option to rebalance the cluster manually at a later time. BEWARE: advanced usage only! | `false` |

### Examples

#### Remove/Unregister node

Remove a proxy node with ID `23kfa10f` from the cluster.

```console
$ ais cluster membership decommission 23kfa10f
Node with ID "23kfa10f" has been successfully removed from the cluster.
```

To also end the `aisnode` process on a given node, use the `shutdown` command:
```console
$ ais cluster membership shutdown 23kfa10f
```

#### Temporarily put a node in maintenance

```console
$ ais show cluster
PROXY            MEM USED %      MEM AVAIL       UPTIME
202446p8082      0.09%           31.28GiB        70s
279128p8080[P]   0.11%           31.28GiB        80s

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME
147665t8084      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               70s
165274t8087      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               70s

$ ais cluster membership start-maintenance 147665t8084
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
$ ais cluster membership stop-maintenance 147665t8084
$ ais show cluster
PROXY            MEM USED %      MEM AVAIL       UPTIME
202446p8082      0.09%           31.28GiB        80s
279128p8080[P]   0.11%           31.28GiB        90s

TARGET           MEM USED %      MEM AVAIL       CAP USED %      CAP AVAIL       CPU USED %      REBALANCE       UPTIME
147665t8084      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               80s
165274t8087      0.10%           31.28GiB        16%             2.458TiB        0.12%           -               80s
```

## Attach remote cluster

`ais cluster attach UUID=URL [UUID=URL...]`

or

`ais cluster attach ALIAS=URL [ALIAS=URL...]`

Attach a remote AIS cluster to this one by the remote cluster public URL. Alias(a user-defined name) can be used instead of cluster UUID for convenience.
For more details and background on *remote clustering*, please refer to this [document](/docs/providers.md).

### Examples

First cluster is attached by its UUID, the second one gets user-friendly alias.

```console
$ ais cluster attach a345e890=http://one.remote:51080 two=http://two.remote:51080`
```

## Detach remote cluster

`ais cluster detach UUID|ALIAS`

Detach a remote cluster from AIS storage by its alias or UUID.

### Examples

```console
$ ais cluster detach two
```

## Show remote clusters

`ais show remote-cluster`

Show details about attached remote clusters.

### Examples
The following two commands attach and then show remote cluster at the address `my.remote.ais:51080`:

```console
$ ais cluster attach alias111=http://my.remote.ais:51080
Remote cluster (alias111=http://my.remote.ais:51080) successfully attached
$ ais show remote-cluster
UUID      URL                     Alias     Primary         Smap  Targets  Online
eKyvPyHr  my.remote.ais:51080     alias111  p[80381p11080]  v27   10       yes
```

Notice that:

* user can assign an arbitrary name (aka alias) to a given remote cluster
* the remote cluster does *not* have to be online at attachment time; offline or currently not reachable clusters are shown as follows:

```console
$ ais show remote-cluster
UUID        URL                       Alias     Primary         Smap  Targets  Online
eKyvPyHr    my.remote.ais:51080       alias111  p[primary1]     v27   10       no
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```

Notice the difference between the first and the second lines in the printout above: while both clusters appear to be currently offline (see the rightmost column), the first one was accessible at some earlier time and therefore we do show that it has (in this example) 10 storage nodes and other details.

To `detach` any of the previously configured association, simply run:

```console
$ ais cluster detach alias111
$ ais show remote-cluster
UUID        URL                       Alias     Primary         Smap  Targets  Online
<alias222>  <other.remote.ais:51080>            n/a             n/a   n/a      no
```
