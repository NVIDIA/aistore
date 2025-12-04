## Table of Contents

- [`ais config cluster`](#ais-config-cluster)
- [`ais config node`](#ais-config-node)
- [Example: show config, update config](#example-show-config-update-config)
- [Example: show specific config section (flat and JSON)](#example-show-specific-config-section-flat-and-json)
- [Configuration inheritance](#configuration-inheritance)
- [Show configuration](#show-configuration)
  - [Cluster configuration](#cluster-configuration)
  - [Node configuration](#node-configuration)
  - [Examples](#examples)
- [Update cluster configuration](#update-cluster-configuration)
- [Update node configuration](#update-node-configuration)
- [Reset configuration](#reset-configuration)
- [CLI configuration](#cli-configuration)
  - [Show CLI configuration](#show-cli-configuration)
  - [Update CLI configuration](#update-cli-configuration)

---

There are two main configuration-managing commands, each with multiple subcommands:

1. `ais config` - show and update configuration
2. `ais show config` - show configuration

> As always, subcommands will reveal themselves via tab completion (`<TAB-TAB>`).

In brief:

```console
$ ais config --help
NAME:
   ais config - Configure AIS cluster and individual nodes (in the cluster); configure CLI (tool)

USAGE:
   ais config command [arguments...] [command options]

COMMANDS:
   show     show CLI, cluster, or node configurations (nodes inherit cluster and have local)
   cluster  configure AIS cluster
   node     configure AIS node
   reset    reset (cluster | node | CLI) configuration to system defaults
   cli      display and change AIS CLI configuration

OPTIONS:
   --help, -h  show help
```

> `ais config show` is an alias for `ais show config` — both can be used interchangeably.

Here are a few usage examples:

## `ais config cluster`

```console
$ ais config cluster --help
NAME:
   ais config cluster - Configure AIS cluster.
   Examples:
     - 'ais config cluster --json'                    - show entire cluster config in JSON;
     - 'ais config cluster log'                       - show 'log' section;
     - 'ais config cluster log.level 4'               - set log level to 4;
     - 'ais config cluster log.modules ec xs'         - elevate verbosity for selected modules;
     - 'ais config cluster features S3-API-via-Root'  - enable feature flag;
     - 'ais config cluster features none'             - reset all feature flags;
     - 'ais config cluster log.modules none'          - reset log modules

USAGE:
   ais config cluster KEY=VALUE [KEY=VALUE...] [command options]

OPTIONS:
   --json, -j   JSON input/output
   --transient  Update config in memory without storing the change(s) on disk
   --help, -h   Show help
```

## `ais config node`

```console
$ ais config node --help
NAME:
   ais config node - Configure AIS node.
   Each node in the cluster has 'inherited' (from cluster config) and 'local' configuration.
   Nodes can override inherited defaults with local values; use caution: local changes
   may cause inconsistent behavior across the cluster. Examples:
     - 'ais config node NODE local --json'              - show node's local config in JSON;
     - 'ais config node NODE inherited log'             - show 'log' section (inherited);
     - 'ais config node NODE local host_net --json'     - show node's network config;
     - 'ais config node NODE log.level 4'               - set node's log level;
     - 'ais config node NODE log.modules none'          - reset log modules;
     - 'ais config node NODE disk.iostat_time_long=4s'  - update disk timing (in re: "disk utilization smoothing")

USAGE:
   ais config node NODE_ID KEY=VALUE [KEY=VALUE...] [command options]

OPTIONS:
   --json, -j   JSON input/output
   --transient  Update config in memory without storing the change(s) on disk
   --help, -h   Show help
```

## Example: show config, update config

```console
# show `ais config` subcommands:
$ ais config <TAB-TAB>
cli    cluster    node    reset    show
```

```console
# select `cluster` configuration and see usage and options
$ ais config cluster --help
```

```console
# show the entire cluster configuration in JSON
$ ais config cluster --json
```

```console
# show one selected section (e.g., checksum) from the cluster config
$ ais config cluster checksum
```

```console
# update one value (e.g., checksum type)
$ ais config cluster checksum.type=md5

# same using JSON-formatted values
$ ais config cluster checksum.type='{"type":"md5"}'
```

More examples:

```console
$ ais config cluster log.level 4
PROPERTY         VALUE
log.level        4
log.max_size     4MiB
log.max_total    128MiB
log.flush_time   40s
log.stats_time   1m
log.to_stderr    false

Cluster config updated

$ ais config cluster log.modules <TAB-TAB>
transport    memsys       fs           ec           ios          backend      mirror       downloader   s3
ais          cluster      reb          stats        xs           space        dsort        etl          none

$ ais config cluster log.modules space,s3
PROPERTY         VALUE
log.level        4 (modules: space,s3)
log.max_size     4MiB
log.max_total    128MiB
log.flush_time   40s
log.stats_time   1m
log.to_stderr    false

Cluster config updated
```

> **Note:** Single quotes are strongly recommended when values contain spaces, wildcards, or double quotes.

## Example: show specific config section (flat and JSON)

```console
$ ais config cluster ec
PROPERTY                 VALUE
ec.objsize_limit         262144
ec.compression           never
ec.bundle_multiplier     2
ec.data_slices           1
ec.parity_slices         1
ec.enabled               false
ec.disk_only             false

# same in JSON:

$ ais config cluster ec --json

    "ec": {
        "objsize_limit": 262144,
        "compression": "never",
        "bundle_multiplier": 2,
        "data_slices": 1,
        "parity_slices": 1,
        "enabled": false,
        "disk_only": false
    }
```

## Configuration inheritance

AIS supports configuration **inheritance** with **local override**.

At any point in time there is a single, protected and replicated version of the cluster configuration. When a new cluster gets deployed, all nodes inherit the same initial configuration with identical default values. When a new node joins the cluster, it receives the current version of the cluster configuration.

However, you can select any node and override any inherited value. Note that if the corresponding value later changes at the cluster level, the node's override takes precedence — the cluster-level update won't apply to that node.

> In other words, overriding inherited configuration on the node level breaks future inheritance for that setting.

> Use `ais config reset` to remove all previous overrides.

All configuration updates are persistent by default. Use the `--transient` flag to update in memory only (changes won't persist across reboots).

See also: [AIStore Configuration](/docs/configuration.md)

## Show configuration

```console
# Select what to show: CLI config, cluster config, or any node's config:

$ ais show config <TAB-TAB>
cli           cluster       p[kdQp8080]   t[NBzt8081]
```

> Target nodes have the `t` prefix; gateways (proxies) have the `p` prefix.

```console
# For cluster config, use `TAB` completion to select a named section, or press Enter:

$ ais show config cluster <TAB-TAB>

downloader         net                proxy              config_version     keepalivetracker
checksum           disk               arch               tracing            fshc
ec                 get_batch          rebalance          tcb                resilver
transport          uuid               timeout            tco                ext
features           rate_limit         space              memsys             auth
versioning         log                lru                lastupdate_time    mirror
write_policy       chunks             client             distributed_sort   periodic
```

```console
$ ais show config cluster --help
NAME:
   ais show config - show CLI, cluster, or node configurations (nodes inherit cluster and have local)

USAGE:
   ais show config cli | cluster [CONFIG SECTION OR PREFIX] | [command options]
      NODE_ID [ inherited | local | all [CONFIG SECTION OR PREFIX ] ]

OPTIONS:
   --json, -j  json input/output
   --help, -h  show help
```

```console
# For a specific node, choose inherited, local, or both:

$ ais show config t[NBzt8081] <TAB-TAB>
inherited   local
```

### Cluster configuration

`ais show config cluster [CONFIG_PREFIX]`

Display the cluster configuration. If `CONFIG_PREFIX` is given, only configurations matching the prefix will be shown.

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Node configuration

`ais show config NODE_ID [inherited | local] [CONFIG_PREFIX]`

Display the node's configuration. Node configuration consists of two parts:

- **inherited** — cluster configuration (may include local overrides)
- **local** — node-specific settings that override cluster defaults

If `CONFIG_PREFIX` is given, only configurations matching the prefix will be shown.

When showing inherited config, the output includes an extra column with global values:

- `-` — local and global values are the same (not overridden)
- `N/A` — option is local-only and doesn't exist in global config

| Flag | Type | Description | Default |
| --- | --- | --- | --- |
| `--json, -j` | `bool` | Output in JSON format | `false` |

### Examples

#### Show node's local configuration

> **Note:** Examples in this section show [local-playground](/docs/getting_started.md#local-playground) paths and ports; production deployments will differ.

```console
$ ais show config CASGt8088 local
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

#### Show node's inherited configuration

```console
$ ais show config CASGt8088 inherited
PROPERTY                                 VALUE                                                           DEFAULT
auth.enabled                             false                                                           -
auth.secret                              **********                                                      -
backend.conf                             map[]                                                           -
checksum.enable_read_range               false                                                           -
checksum.type                            xxhash                                                          -
checksum.validate_cold_get               true                                                            -
checksum.validate_obj_move               false                                                           -
checksum.validate_warm_get               false                                                           -
client.client_long_timeout               30m                                                             -
# only 10 lines of output shown
```

#### Show cluster LRU config section

```console
$ ais show config cluster lru
PROPERTY                 VALUE
lru.dont_evict_time      1s
lru.capacity_upd_time    10m
lru.enabled              true

$ ais show config cluster space
PROPERTY                 VALUE
space.cleanupwm          70
space.lowwm              80
space.highwm             90
space.out_of_space       95
```

## Update cluster configuration

`ais config cluster NAME=VALUE [NAME=VALUE...]`

Use tab completion to discover available settings:

```console
$ ais config cluster <TAB-TAB>
Display all 108 possibilities? (y or n)

$ ais config cluster timeout.<TAB-TAB>
timeout.cplane_operation    timeout.max_host_busy       timeout.send_file_time
timeout.join_startup_time   timeout.max_keepalive       timeout.startup_time
```

Specify name-value pairs separated by space or `=`. To see the current value before updating, just press Enter:

```console
$ ais config cluster lru.enabled
PROPERTY         VALUE
lru.enabled      true

$ ais config cluster lru.enabled=false
{
    "lru.enabled": "false"
}

Cluster config updated
```

### Set multiple config values

```console
$ ais config cluster periodic.stats_time=10s disk.disk_util_low_wm=40
Config has been updated successfully.
```

## Update node configuration

`ais config node NODE_ID [inherited | local] NAME=VALUE [NAME=VALUE...]`

Steps:

1. Select a node (or use `<TAB-TAB>` to complete)
2. Select `inherited` to update cluster-level values, or `local` for node-specific settings
3. Specify name-value pairs

> When updating inherited values, all previous overrides can be undone with `ais config reset`.

### Set multiple config values

```console
$ ais config node CMhHp8082 periodic.stats_time=10s disk.disk_util_low_wm=40
Config has been updated successfully.
```

## Reset configuration

`ais config reset [NODE_ID]`

Reset configuration back to cluster defaults, removing all local overrides.

```console
# Reset all nodes to cluster defaults:

$ ais config reset
config successfully reset for all nodes
```

```console
# Reset a specific node:

$ ais config reset CMhHp8082
config for node "CMhHp8082" successfully reset
```

## CLI configuration

The CLI tool has its own configuration, separate from cluster configuration.

> **Note:** Examples in this section show [local-playground](/docs/getting_started.md#local-playground) paths and ports; production deployments will differ.

### Show CLI configuration

`ais config cli [--path] [--json]`

Display the current CLI configuration. Use `--path` to show only the config file location.

```console
$ ais config cli

PROPERTY                         VALUE
aliases                          cp => 'bucket cp'; create => 'bucket create'; evict => 'bucket evict';
                                 ls => 'bucket ls'; rmb => 'bucket rm'; start => 'job start';
                                 blob-download => 'job start blob-download'; download => 'job start download';
                                 dsort => 'job start dsort'; stop => 'job stop'; wait => 'job wait';
                                 get => 'object get'; mpu => 'object multipart-upload'; prefetch => 'object prefetch';
                                 put => 'object put'; rmo => 'object rm'; space-cleanup => 'storage cleanup';
                                 scrub => 'storage validate'
auth.url                         http://127.0.0.1:52001
cluster.client_ca_tls
cluster.client_crt
cluster.client_crt_key
cluster.default_ais_host         http://127.0.0.1:8080
cluster.default_docker_host      http://172.50.0.2:8080
cluster.skip_verify_crt          false
cluster.url                      http://127.0.0.1:8080
default_provider                 ais
no_color                         false
no_more                          false
timeout.http_timeout             0s
timeout.tcp_timeout              60s
verbose                          false
```

```console
$ ais config cli --path
/home/user/.ais/cli/cli.json
```

### Update CLI configuration

`ais config cli NAME=VALUE [NAME=VALUE...]`

The configuration file is updated only if all new options are applied without errors. If an option name doesn't exist or a value is incorrect, the operation is aborted.

```console
$ ais config cli timeout.tcp_timeout 61s
"timeout.tcp_timeout" set to: "61s" (was: "60s")

$ ais config cli --json
{
    "cluster": {
        "url": "http://127.0.0.1:8080",
        "default_ais_host": "http://127.0.0.1:8080",
        "default_docker_host": "http://172.50.0.2:8080",
        "client_crt": "",
        "client_crt_key": "",
        "client_ca_tls": "",
        "skip_verify_crt": false
    },
    "timeout": {
        "tcp_timeout": "60s",
        "http_timeout": "0s"
    },
    "auth": {
        "url": "http://127.0.0.1:52001"
    },
    "aliases": {
        "wait": "job wait",
        "cp": "bucket cp",
        "ls": "bucket ls",
        "prefetch": "object prefetch",
        "rmo": "object rm",
        "dsort": "job start dsort",
        "get": "object get",
        "rmb": "bucket rm",
        "start": "job start",
        "put": "object put",
        "stop": "job stop",
        "blob-download": "job start blob-download",
        "create": "bucket create",
        "download": "job start download",
        "evict": "bucket evict"
    },
    "default_provider": "ais",
    "no_color": false,
    "verbose": false,
    "no_more": false
}
```
