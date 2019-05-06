## Daemon/Cluster 

The CLI allows users to interact with AIS daemons or cluster.

## Command List

### list

`ais list`

Lists all of the Daemons in the AIS cluster

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose, -v` | `bool` | verbose option | `false` |

### status

`ais status [DAEMON_TYPE]`

Returns the status of the `DAEMON_TYPE`. `DAEMON_TYPE` is either `proxy`, `target`, or `DAEMON_ID`. If `DAEMON_TYPE` is not set, it will return the status of all the daemons in the AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |
| `--refresh` | `string` | time.Duration string that specifies the amount of time between reports | `1s` |
| `--count` | `int` | total number of generated reports | `1` |

Examples:

* `ais status --count 5` - displays 5 reports with statuses of all daemons in the cluster with 1s interval between each report
* `ais status proxy --count 5 --refresh 10s` - displays 5 reports with statuses of all proxies in the cluster with 10s interval between each report
* `ais status target --refresh 2s` - displays a continuous report with statuses of all targets in the cluster with 2s interval between each report

### config

`ais config [DAEMON_ID]`

Returns the configuration of `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the configuration of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

### setconfig

`ais setconfig [DAEMON_ID] [list of key=value]`

Set configurations for a specific daemon or the entire cluster via key-value pairs. To set configurations for the entire cluster, use `cluster` as the `DAEMON_ID`. For the list of available runtime configurations, see [here](../../docs/configuration.md#runtime-configuration).

Example:

`ais setconfig cluster stats_time=10s`

Sets the `stats_time` configuration to `10s` for the entire cluster.

### smap

`ais smap [DAEMON_ID]`
Returns the cluster map (smap) of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the smap of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

### stats

`ais stats [DAEMON_ID]`
Returns the stats of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the stats of a current primary proxy and all the targets in the cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |
| `--refresh` | `string` | time.Duration string that specifies the amount of time between reports | `1s` |
| `--count` | `int` | total number of generated reports | `1` |

Examples:

* `ais stats --count 5` - displays 5 reports with statistics of a current primary proxy and all targets in the cluster with 1s interval between each report
* `ais stats --count 5 --refresh 10s` - displays 5 reports with statistics of a current primary proxy and all targets in the cluster with 10s interval between each report
* `ais stats --refresh 2s` - displays a continuous report with statistics of a current primary proxy and all targets in the cluster with 2s interval between each report
