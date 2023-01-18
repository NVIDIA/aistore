---
layout: post
title: CONFIG
permalink: /docs/cli/config
redirect_from:
 - /cli/config.md/
 - /docs/cli/config.md/
---

This document details AIStore configuration management using AIS CLI.

Generally, there are two main commands, each having multiple subcommands and sub-subcommands:

1. `ais config`		- show and update configuration
2. `ais show config`	- show configuration

> As always, the subcommands of the `ais config` and, respectively, `ais show config` will reveal themselves as completions (upon `<TAB-TAB>`).

Here's a quick usage example:

```console
# show `ais config` subcommands:
$ ais config <TAB-TAB>
cli    cluster    node    reset    show

# select `cluster` configuration and see usage and options
$ ais config cluster --help

# show the entire cluster configuration in JSON
$ ais config cluster --json

# show one selected section (e.g., checksum) from the cluster config
$ ais config cluster checksum

# update one value (e.g., checksum type)
$ ais config cluster checksum=md5

# using JSON-formatted values, update backend configuration;
# for supported backend providers, see docs/providers.md
$ ais config cluster backend.conf='{"gcp":{}, "aws":{}}'
```

Further, as far as configuration, AIS supports **inheritance** and **local override**.

Here's how it works:

At any point in time there is a single, protected and replicated, version of the cluster configuration. When a new cluster gets deployed, all clustered nodes inherit the same (initial) version - identical default values.

Subsequently, when a new node joins cluster it will also receive the current version of the cluster configuration.

On the other hand, it is possible at any point in time to select any node and override (any selected) inherited value - the value *inherited* by the node with its replica of the cluster config.

Note, however: if and when the corresponding value changes on the cluster level, the node's override will take precedence - the specific update won't apply (to this node).

> In other words, overriding inherited (cluster) configuration on the node level breaks the future inheritance.

> Use `ais config reset` to remove all previous overrides.

Finally, note that all configuration updates are, by default, persistent. Use the `--transient` flag to make them transient - i.e., in memory only, i.e., *not* persisting across reboots.

See also:

* [AIStore Configuration](/docs/configuration.md)

## Table of Contents

- [Show configuration](#show-configuration)
- [`ais show config`](#ais-show-config)
- [Update cluster configuration](#update-cluster-configuration)
- [Update node configuration](#update-node-configuration)
- [Reset configuration](#reset-configuration)
- [CLI own configuration](#cli-own-configuration)

## Show configuration

The command `ais show config` is structured as follows.

```console
# 1. Select to show: CLI config, cluster config, or the config of any of the clustered nodes, e.g.:

$ ais show config <TAB-TAB>
cli           cluster       p[kdQp8080]   t[NBzt8081]
```

> Notice here (and everywhere) that target nodes have `t` prefix, while AIS gateways (aka proxies) start with `p`.

```console
# 2. For the cluster config, select a named section, or simply press Enter. Following
# is the complete list of configuration sections resulting from pressing <TAB-TAB>:

$ ais show config cluster
auth               disk               features           lru                proxy              timeout
backend            distributed_sort   fshc               memsys             rebalance          transport
checksum           downloader         keepalivetracker   mirror             resilver           uuid
client             ec                 lastupdate_time    net                space              versioning
config_version     log                periodic           tcb                write_policy
```

```console
# 3. Tip: help is available at any point. For instance, as stated above, you could select a section.
# But you could also run `ais show config cluster` with no section selected,
# with or without `--json` option to format the output:

$ ais show config cluster --help
NAME:
   ais show config - show CLI, cluster, or node configurations (nodes inherit cluster and have local)

USAGE:
   ais show config [command options] cli | cluster [CONFIG SECTION OR PREFIX] |
      DAEMON_ID [ cluster | local | all [CONFIG SECTION OR PREFIX ] ]

OPTIONS:
   --json, -j  json input/output
   --help, -h  show help
```

```console
# 4. Finally, for any specific node you can show its inherited config (where some of the values *may* be overridden) and its local one:

$ ais show config t[NBzt8081]
inherited   local
```

## `ais show config`

As stated above, the command further splits as follows:

`ais show config cluster` or `ais show config cli` or `ais show config [DAEMON_ID]`

Node configuration consists of two parts:

- global cluster configuration which is the same across the cluster
- local daemon configuration which overrides the global one.

### Cluster configuration

`ais show cluster config [CONFIG_PREFIX]`

Display the cluster configuration. If `CONFIG_PREFIX` is given, only that configurations matching the prefix will be shown.

#### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Node configuration

`ais show config DAEMON_ID [CONFIG_PREFIX]`

Display the actual daemon configuration. If `CONFIG_PREFIX` is given, only the configurations matching the prefix will be shown.
The output includes extra column with global values. Some values in the column have special meaning:

- `-` - the local and global values are the same, the option is not overridden
- `N/A` - the option is local-only and does not exist in global config

#### Options

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show node's local configuration

Display all local configurations of the node with ID `CASGt8088`

```console
$ ais show config local CASGt8088
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
$ ais show config inherited CASGt8088
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
$ ais show config Gpuut8085 lru
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

## Update cluster configuration

`ais config cluster NAME=VALUE [NAME=VALUE...]`

Alternatively:

```console
$ ais config cluster <TAB-TAB>
Display all 108 possibilities? (y or n)

$ ais config cluster time<TAB-TAB>
$ ais config cluster timeout.<TAB-TAB>
timeout.cplane_operation    timeout.max_host_busy       timeout.send_file_time
timeout.join_startup_time   timeout.max_keepalive       timeout.startup_time
```

And so on.

Updating is done by specifying name-value pairs. Use completions to help you remind (and/or type) the name and specify the new value. Use space or the `=` sign to delineate names from values.

**NOTE:** to see the current (pre-update) value, simply press Enter.

For example:

```console
$ ais config cluster lru.enabled
PROPERTY         VALUE
lru.enabled      true

$ ais config cluster lru.enabled=false
{
	    "lru.enabled": "false"
}

cluster config updated
```

### Set multiple config values in one shot

Change `periodic.stats_time` and `disk.disk_util_low_wm` config values for the entire cluster.

```console
$ ais config cluster periodic.stats_time=10s disk.disk_util_low_wm=40
Config has been updated successfully.
```

## Update node configuration

`ais config node DAEMON_ID inherited NAME=VALUE [NAME=VALUE...]`

or

`ais config node DAEMON_ID local NAME=VALUE [NAME=VALUE...]`


Usually, the steps:

1. Select a node or use `<TAB-TAB>` to complete the selection.
2. Next, select `inherited` to update cluster-level values. Alternatively, type or select `local`.
3. Update selected value. Name and value can be separated either with `=` character or with a space.

> When updating inherited values, keep in mind: all previous overrides can be undone using `ais config reset` command.

### Set multiple config values

```console
# Change `periodic.stats_time` and `disk.disk_util_low_wm` config values for node CMhHp8082.

$ ais config node CMhHp8082 periodic.stats_time=10s disk.disk_util_low_wm=40
Config has been updated successfully.
```

## Reset configuration

`ais config reset [DAEMON_ID]`

Reset configuration for a specific daemon or the entire cluster back to the cluster configuration.
That is, all local overrides will be removed and the cluster configuration will be applied to all nodes.
To reset the configuration for the entire cluster, do not specify a `DEAMON_ID` argument.

```console
# Discard local overrides for all nodes:

$ ais config reset
config successfully reset for all nodes
```

```console
# Reset node's configuration to the current cluster-level values:

$ ais config reset CMhHp8082
config for node "CMhHp8082" successfully reset
```

## CLI own configuration

CLI (tool) has configuration of its own. CLI (tool) can be used to view and update its own config.

### Show CLI configuration

`ais config cli show [--path] [--json]`

Display the current CLI configuration.
If `--path` is set, display only the path to the CLI configuration file.

#### Examples

```console
$ ais config cli show
PROPERTY                         VALUE
aliases                          map[get:object get ls:bucket ls put:object put]
auth.url                         http://127.0.0.1:52001
cluster.default_ais_host         http://127.0.0.1:8080
cluster.default_docker_host      http://172.50.0.2:8080
cluster.skip_verify_crt          false
cluster.url                      http://127.0.0.1:8080
default_provider                 ais
timeout.http_timeout             0s
timeout.tcp_timeout              60s

$ ais config cli show --path
/home/user/.ais/cli/cli.json
```

### Change CLI configuration

`ais config cli set NAME=VALUE [NAME=VALUE...]`

Modify the CLI configuration. The configuration file is updated only if **all** new options are applied without errors.
If an option name does not exist or value is incorrect the operation is aborted.

#### Examples

```console
$ ais config cli set timeout.tcp_timeout 61s
"timeout.tcp_timeout" set to: "61s" (was: "60s")

$ ais config cli show --json
{
    "cluster": {
        "url": "http://127.0.0.1:8080",
        "default_ais_host": "http://127.0.0.1:8080",
        "default_docker_host": "http://172.50.0.2:8080",
        "skip_verify_crt": false
    },
    "timeout": {
        "tcp_timeout": "61s",
        "http_timeout": "0s"
    },
    "auth": {
        "url": "http://127.0.0.1:52001"
    },
    "aliases": {
        "get": "object get",
        "ls": "bucket ls",
        "put": "object put"
    },
    "default_provider": "ais"
}
```
