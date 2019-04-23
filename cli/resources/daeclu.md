## Daemon/Cluster 

The CLI allows users to interact with AIS daemons or cluster.

## Command List

### config

`ais config [DAEMON_ID]`

Returns the configuration of `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the configuration of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

### list

`ais list`

Lists all of the Daemons in the AIS cluster

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--verbose, -v` | `bool` | verbose option | `false` |

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
Returns the stats of the `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the stats of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |
| `--watch` | `bool` | watch the progress of metric | `false` |
| `--refresh` | `string` | time.Duration string for how often to refresh | `5s` |

### status

`ais status [OPTION]`

Returns the status of the `OPTION`. `OPTION` is either `proxy`, `target`, or `DAEMON_ID`. If `OPTION` is not set, it will return the status all the daemons in the AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |
| `--watch` | `bool` | watch the progress of metric | `false` |
| `--refresh` | `string` | time.Duration string for how often to refresh | `5s` |