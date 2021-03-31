# CLI Reference for Configurations
This section lists configuration management operations the AIS CLI, with `ais config`.

## Table of Contents
- [Show configuration](#show-configuration)
- [Set cluster configuration](#set-cluster-configuration)
- [Set node configuration](#set-node-configuration)
- [Reset configuration](#reset-configuration)

## Show configuration

A daemon configuration consists of two parts:

- global cluster configuration which is the same across the cluster
- local daemon configuration which overrides the cluster one.

### Cluster configuration

`ais show cluster config [CONFIG_PREFIX]`

Display the cluster configuration. If `CONFIG_PREFIX` is given, only that configurations matching the prefix will be shown.

To see all configurations, specify `--type all` and no prefix. You can also filter the type to `--type cluster` and `--type local`.

#### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--type` | `string` | Show the specified configuration values. One of `all`, `cluster`, `local`. | N/A |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Daemon configuration

`ais show config DAEMON_ID [CONFIG_PREFIX]`

Display the actual daemon configuration. If `CONFIG_PREFIX` is given, only that configurations matching the prefix will be shown.
The output includes extra column with global values. Some values in the column have special meaning:

- `-` - the local and global values are the same, the option is not overridden
- `N/A` - the option is local-only and does not exist in global config

#### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--all` | `bool` | Show all configuration values | `false` |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show local configurations on a node

Display all local configurations of the node with ID `CASGt8088`

```console
$ ais show config CASGt8088 --type local
PROPERTY                         VALUE
confdir                          /home/divaturi/.ais8
log_dir                          /tmp/ais/8/log
host_net.hostname                
host_net.hostname_intra_control  
host_net.hostname_intra_data     
host_net.port                    8088
host_net.port_intra_control      9088
host_net.port_intra_data         10088
fspaths.paths                    /tmp/ais/mp1/8,/tmp/ais/mp2/8,/tmp/ais/mp3/8,/tmp/ais/mp4/8,/tmp/ais/mp5/8
test_fspaths.root                /tmp/ais
test_fspaths.count               5
test_fspaths.instance            8
```

#### Show cluster configurations on a node

Display all cluster configurations (and overrides) of the node with ID `CASGt8088`

```console
$ ais show config CASGt8088 --type cluster
PROPERTY                                 VALUE                                                           DEFAULT
auth.enabled                             false                                                           -
auth.secret                              aBitLongSecretKey                                               -
backend.conf                             map[]                                                           -
checksum.enable_read_range               false                                                           -
checksum.type                            xxhash                                                          -
checksum.validate_cold_get               true                                                            -
checksum.validate_obj_move               false                                                           -
checksum.validate_warm_get               false                                                           -
client.client_long_timeout               30m                                                             -
# only 10 lines of output shown
```

#### Show daemon LRU config section

Display only the LRU config section of the node with ID `Gpuut8085`

```console
$ ais show config Gpuut8085 lru -v
PROPERTY                 VALUE   DEFAULT
lru.capacity_upd_time    10m     -
lru.dont_evict_time      120m    -
lru.enabled              false   true
lru.highwm               90      -
lru.lowwm                75      -
lru.out_of_space         95      -
```

#### Show cluster LRU config section

Display only the LRU config section of the global config

```console
$ ais show cluster config lru
PROPERTY                 VALUE
lru.lowwm                75
lru.highwm               90
lru.out_of_space         95
lru.dont_evict_time      120m
lru.capacity_upd_time    10m
lru.enabled              true
```

## Set cluster configuration

`ais config cluster KEY=VALUE [KEY=VALUE...]`

Set a configuration on the global configuration by specifying key-value pairs.
Note that local overrides (set with `ais config node`) will persist. Use `ais config reset` to remove all overrides.
For the list of available runtime configurations, see [here](/docs/configuration.md#runtime-configuration).

Key and value can be separated with `=` character or with a space.
The former case supports both short and fully-qualified option names.
The latter case requires the key to be a fully-qualified name.

### Examples

#### Set multiple config values

Change `periodic.stats_time` and `disk.disk_util_low_wm` config values for the entire cluster.

```console
$ ais config cluster periodic.stats_time=10s disk.disk_util_low_wm=40
Config has been updated successfully.
```

## Set node configuration

`ais config node DAEMON_ID KEY=VALUE [KEY=VALUE...]`

Set a local configuration override for a specific daemon by specifying key-value pairs.
Even when the global (i.e. cluster) configuration is updated, these overrides will persist. Use `ais config reset` to remove all overrides.
For the list of available runtime configurations, see [here](/docs/configuration.md#runtime-configuration).

Key and value can be separated with `=` character or with a space.
The former case supports both short and fully-qualified option names.
The latter case requires the key to be a fully-qualified name.

### Examples

#### Set multiple config values

Change `periodic.stats_time` and `disk.disk_util_low_wm` config values for node CMhHp8082.

```console
$ ais config node CMhHp8082 periodic.stats_time=10s disk.disk_util_low_wm=40
Config has been updated successfully.
```

## Reset configuration

`ais config reset [DAEMON_ID]`

Reset configuration for a specific daemon or the entire cluster back to the cluster configuration.
That is, all local overrides will be removed and the cluster configuration will be applied to all nodes.
To reset the configuration for the entire cluster, do not specify a `DEAMON_ID` argument.

### Examples

#### Reset configuration for all nodes

```console
$ ais config reset
config successfully reset for all nodes
```

#### Reset configuration for one node

```console
$ ais config reset CMhHp8082
config for node "CMhHp8082" successfully reset
```
