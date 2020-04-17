The CLI allows users to interact with AIS daemons or cluster.
A daemon is either proxy or target. 

## Cluster or Daemon status

`ais status [DAEMON_TYPE]|[DAEMON_ID]`

Return the status of the `DAEMON_TYPE` or `DAEMON_ID`. `DAEMON_TYPE` is either `proxy` or `target`. If `DAEMON_TYPE` is not set, it will return the status of all the daemons in the AIS cluster.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

## Show cluster map

`ais show smap [DAEMON_ID]`

Show a copy of the cluster map (smap) present on `DAEMON_ID`. If `DAEMON_ID` isn't given, it will show the smap of the daemon that the `AIS_URL` points at.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show smap from a given node

Show smap copy of daemon with ID `26830p8083`.

```console
$ ais show smap 26830p8083
DaemonID	 Type	 PublicURL
26830p8083	 proxy	 http://192.168.0.178:8083
638285p8080[P]	 proxy	 http://192.168.0.178:8080
699197p8084	 proxy	 http://192.168.0.178:8084
774822p8081	 proxy	 http://192.168.0.178:8081
87405p8082	 proxy	 http://192.168.0.178:8082

DaemonID	 Type	 PublicURL
130709t8088	 target	 http://192.168.0.178:8088
613132t8085	 target	 http://192.168.0.178:8085
634992t8087	 target	 http://192.168.0.178:8087
792959t8089	 target	 http://192.168.0.178:8089
870250t8086	 target	 http://192.168.0.178:8086

Non-Electable:

PrimaryProxy: 638285p8080	 Proxies: 5	 Targets: 5	 Smap Version: 10
```

## Node details

`ais show node [DAEMON_ID]`

Show details about `DAEMON_ID`. If `DAEMON_ID` is omitted, shows details about the current primary proxy and all targets in the cluster.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |

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

`ais join proxy IP:PORT [DAEMON_ID]`

Join a proxy in the cluster. If `DAEMON_ID` isn't given, it will be randomly generated.

`ais join target IP:PORT [DAEMON_ID]`

Join a target in the cluster. If `DAEMON_ID` isn't given, it will be randomly generated.

### Examples

#### Join node

Join a proxy node with ID `23kfa10f` and socket address `192.168.0.185:8086`

```console
$ ais join proxy 192.168.0.185:8086 23kfa10f
Proxy with ID "23kfa10f" successfully joined the cluster.
```

## Remove a node

`ais rm node DAEMON_ID`

Remove an existing node from the cluster.

### Examples

#### Remove/Unregister node

Remove a proxy node with ID `23kfa10f` from the cluster.

```console
$ ais rm node 23kfa10f
Node with ID "23kfa10f" has been successfully removed from the cluster.
```

## Show config

`ais show config DAEMON_ID [CONFIG_SECTION]`

Display the configuration of `DAEMON_ID`. If `CONFIG_SECTION` is given, only that specific section will be shown.

### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show LRU config section

Display only the LRU config section of the node with ID `23kfa10f`

```console
$ ais show config 23kfa10f lru
LRU Config
 Low WM:		75
 High WM:		90
 Out-of-Space:		95
 Don't Evict Time:	120m
 Capacity Update Time:	10m
 Enabled:		true
```

## Set config

`ais set config [DAEMON_ID] KEY=VALUE [KEY=VALUE...]`

Set configuration for a specific daemon or the entire cluster by specifying key-value pairs.
To set config for the entire cluster, omit the `DEAMON_ID` argument.
For the list of available runtime configurations, see [here](../../../docs/configuration.md#runtime-configuration).

Key and value can be separated with `=` character or with a space.
The former case supports both short and fully-qualified option names.
The latter case requires the key to be a fully-qualified name.

### Examples

#### Set multiple config values

Change `periodic.stats_time` and `disk.disk_util_low_wm` config values for the entire cluster.

```console
$ ais set config periodic.stats_time 10s disk.disk_util_low_wm 40
Config has been updated successfully.
```
