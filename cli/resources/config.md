## Config

The CLI allows users to interact with configurations of AIS daemons or cluster.

## Command List

### get

`ais config get [DAEMON_ID]`

Displays the configuration of `DAEMON_ID`. If `DAEMON_ID` is not set, it will return the configuration of the daemon that the `AIS_URL` is pointed at.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |

### set

`ais config set [DAEMON_ID] key=value...`

Set configurations for a specific daemon or the entire cluster via key-value pairs. To set configurations for the entire cluster, omit the `DEAMON_ID` argument. For the list of available runtime configurations, see [here](../../docs/configuration.md#runtime-configuration).

Key and value can be separated with `=` character or with a space. The former case supports both short and full-qualified option names. The latter case requires the key to be full-qualified name.

Example:

Short option names and equal sign as a separator:

`ais config set stats_time=10s disk_util_low_wm=40`

The same command with a space as the separator:

`ais config set periodic.stats_time 10s disk.disk_util_low_wm 40`

Both commands sets the `periodic.stats_time` configuration to `10s` and `disk.disk_util_low_wm` to `40` for the entire cluster.
