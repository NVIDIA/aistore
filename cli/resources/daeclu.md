## Daemon/Cluster 

The CLI allows users to interact with AIS daemons or cluster.

## Command List

### status

`ais status [DAEMON_TYPE]`

Returns the status of the `DAEMON_TYPE`. `DAEMON_TYPE` is either `proxy`, `target`, or `DAEMON_ID`. If `DAEMON_TYPE` is not set, it will return the status of all the daemons in the AIS cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |
| `--refresh` | `string` | time.Duration string that specifies the amount of time between reports | `1s` |
| `--count` | `int` | total number of generated reports | `1` |

#### Examples:
* `ais status --count 5` - displays 5 reports with statuses of all daemons in the cluster with 1s interval between each report
* `ais status proxy --count 5 --refresh 10s` - displays 5 reports with statuses of all proxies in the cluster with 10s interval between each report
* `ais status target --refresh 2s` - displays a continuous report with statuses of all targets in the cluster with 2s interval between each report

Note: more detailed information about target's status can be retreived by [`ais xaction`](./xaction.md) command.

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

#### Examples:
* `ais stats --count 5` - displays 5 reports with statistics of a current primary proxy and all targets in the cluster with 1s interval between each report
* `ais stats --count 5 --refresh 10s` - displays 5 reports with statistics of a current primary proxy and all targets in the cluster with 10s interval between each report
* `ais stats --refresh 2s` - displays a continuous report with statistics of a current primary proxy and all targets in the cluster with 2s interval between each report

### disk

`ais disk [TARGET_ID]`
Returns the disk stats of the `TARGET_ID`. `TARGET_ID` is optional, if not present disk stats for all targets will be returned.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | output in JSON format | `false` |
| `--refresh` | `string` | time.Duration string that specifies the amount of time between reports | `1s` |
| `--count` | `int` | total number of generated reports | `1` |

#### Examples:
* `ais disk --count 5` - displays 5 reports with disk statistics of all targets in the cluster with 1s interval between each report
* `ais disk --count 5 --refresh 10s` - displays 5 reports with disk statistics of all targets in the cluster with 10s interval between each report
* `ais disk --refresh 2s` - displays a continuous report with disk statistics of all targets in the cluster with 2s interval between each report


### node add

`ais node add --daemon-id <value> --daemon-type <value> --public-addr <IP:PORT>`
Adds a new node to the cluster.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--daemon-id` | `string` | unique id for the new node | `""` |
| `--daemon-type` | `string` | type of the node to be added. Either `"target"` or `"proxy"` | `"target"` |
| `--public-addr` | `string` | public socket address to the node, must be in format `IP:PORT` | `""` |

#### Examples:
* `ais node add --daemon-id 23kfa10f --daemon-type proxy --public-addr 192.168.0.185:8086` - adds a new proxy node with id `23kfa10f` which can be contacted at `192.168.0.185:8086`

### node remove

`ais node remove [DAEMON_ID]`
Removes an existing node from the cluster.

#### Examples:
* `ais node remove 23kfa10f` - removes node with id `23kfa10f` from the cluster 
