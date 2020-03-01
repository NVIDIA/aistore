## Daemon/Cluster

The CLI allows users to interact with AIS daemons or cluster.
A daemon is either proxy or target. 

### Cluster or daemon status

`ais status [DAEMON_TYPE]|[DAEMON_ID]`

Return the status of the `DAEMON_TYPE` or `DAEMON_ID`. `DAEMON_TYPE` is either `proxy` or `target`. If `DAEMON_TYPE` is not set, it will return the status of all the daemons in the AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais status --count 5` | Displays 5 reports showing status of all daemons in the cluster with 1s interval between each report |
| `ais status target --refresh 2s` | Displays a continuous report showing status of all targets in the cluster with 2s interval between each report |

### Cluster map

`ais ls smap [DAEMON_ID]`

Show a copy of the cluster map (smap) present on `DAEMON_ID`. If `DAEMON_ID` isn't given, it will show the smap of the daemon that the `AIS_URL` points at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais ls smap 1048575_8084` | Shows smap copy of daemon with ID `1048575_8084` |

### Node details

`ais show node [DAEMON_ID]`

Show details about `DAEMON_ID`. If `DAEMON_ID` is omitted, shows details about the current primary proxy and all targets in the cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais stats node all --count 5` | Displays 5 reports with statistics of the current primary proxy and all targets in the cluster with 1s interval between each report |
| `ais stats node all --count 5 --refresh 10s` | Same as above, but with 10s intervals between each report |
| `ais stats node 1048575_8084 --refresh 2s` | Displays a continuous report with statistics of the node with ID `1048575_8084`, with 2s interval between each report |

### Disk stats

`ais show disk [TARGET_ID]`

Show the disk stats of the `TARGET_ID`. If `TARGET_ID` isn't given, disk stats for all targets will be shown.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |
| `--count` | `int` | Total number of generated reports | `1` |
| `--refresh` | `string` | Time duration between reports | `1s` |
| `--no-headers` | `bool` | Display tables without headers | `false` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais show disk --count 5` | Displays 5 reports with disk statistics of all targets in the cluster with 1s interval between each report |
| `ais show disk --count 5 --refresh 10s` | Same as above, but with 10s intervals between each report |
| `ais show disk 1048575_8084 --refresh 2s` | Displays a continuous report with disk statistics of target with ID `1048575_8084`, with 2s interval between each report |

### Register a node

`ais register proxy IP:PORT [DAEMON_ID]`

Register a proxy in the cluster. If `DAEMON_ID` isn't given, it will be randomly generated.

`ais register target IP:PORT [DAEMON_ID]`

Register a target in the cluster. If `DAEMON_ID` isn't given, it will be randomly generated.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais register proxy 192.168.0.185:8086 23kfa10f` | Registers a proxy node with ID `23kfa10f` and address `192.168.0.185:8086` |

### Remove a node

`ais rm node DAEMON_ID`

Remove an existing node from the cluster.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais rm node 23kfa10f` | Removes node with ID `23kfa10f` from the cluster |

### List config

`ais ls config DAEMON_ID [CONFIG_SECTION]`

Display the configuration of `DAEMON_ID`. If `CONFIG_SECTION` is given, only that specific section will be shown.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

#### Examples

| Command | Explanation |
| --- | --- |
| `ais ls config 844974_8080` | Displays config of the node with ID `844974_8080` |
| `ais ls config 844974_8080 lru` | Displays only the LRU config section of the node with ID `844974_8080` |

### Set config

`ais set config [DAEMON_ID] KEY=VALUE [KEY=VALUE...]`

Set configuration for a specific daemon or the entire cluster by specifying key-value pairs. To set config for the entire cluster, omit the `DEAMON_ID` argument. For the list of available runtime configurations, see [here](../../docs/configuration.md#runtime-configuration).

Key and value can be separated with `=` character or with a space. The former case supports both short and fully-qualified option names. The latter case requires the key to be a fully-qualified name.

#### Examples

| Command | Explanation |
| --- | --- |
| `ais set config stats_time=10s disk_util_low_wm=40` | Sets config for the entire cluster using short names and `=` as a separator |
| `ais set config periodic.stats_time 10s disk.disk_util_low_wm 40` | Sets config for the entire cluster using fully-qualified names and space as a separator |