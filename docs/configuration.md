## Table of Contents
- [Runtime configuration](#runtime-configuration)
- [Configuration persistence](#configuration-persistence)
- [Startup override](#startup-override)
- [Managing filesystems](#managing-filesystems)
- [Disabling extended attributes](#disabling-extended-attributes)
- [Enabling HTTPS](#enabling-https)
- [Filesystem Health Checker](#filesystem-health-checker)
- [Networking](#networking)
- [Reverse proxy](#reverse-proxy)
- [Examples](#examples)

AIStore configuration is consolidated in a single [JSON file](/ais/setup/config.sh) where the configuration sections and the knobs within those sections must be self-explanatory, and the majority of those, except maybe just a few, have pre-assigned default values. The notable exceptions include:

<img src="images/ais-config-1.png" alt="Configuration: TCP port and URL" width="600">

and

<img src="images/ais-config-2-commented.png" alt="Configuration: local filesystems" width="600">

As shown above, the `test_fspaths` section of the configuration corresponds to a **single local filesystem being partitioned** between both *local* and *Cloud* buckets. In other words, the `test_fspaths` configuration option is intended strictly for development.

In production we use an alternative configuration called `fspaths`: the section of the [config](/ais/setup/config.sh) that includes a number of local directories, whereby each directory is based on a different local filesystem.

> Terminology: *mountpath* is a triplet **(local filesystem (LFS), disks that this LFS utilizes, LFS directory)**. The following rules are enforced: 1) different mountpaths use different LFSes, and 2) different LFSes use different disks.

> The terms `fspath` (aka `filesystem path`) and `mountpath` are used interchangeably throughout AIStore docs and sources. When `fspath` configuration is enabled, the 1-to-1 relationship between configured `mountpaths` and local filesystems is enforced and validated at all times.

An example of 12 fspaths (and 12 local filesystems) follows below:

<img src="images/example-12-fspaths-config.png" alt="Example: 12 fspaths" width="160">

## Runtime configuration

First, some basic facts:

* AIS cluster is a collection of nodes - members of the cluster.
* A node can be an AIS proxy (aka gateway) or an AIS target.
* In either case, HTTP request to read (get) or write (set) specific node's configuration will have `/v1/daemon` in its URL path.
* The capability to carry out cluster-wide configuration updates is also supported. The corresponding HTTP URL will have `/v1/cluster` in its path.

> Both `daemon` and `cluster` are the two RESTful resource abstractions supported by the API. Please see [AIS API](http_api.md) for naming conventions, RESTful resources, as well as API reference and details.

* To get the node's up-to-date configuration, execute:

```shell
# curl -X GET 'http://G-or-T/v1/daemon?what=config'
```
where `G-or-T` denotes a **(hostname:port)** pair of **any AIS node** member of the cluster.

This will result in a JSON structure that'll contain all configuration sections and all the named *knobs* - i.e., configuration variables and their current values.

Most configuration options can be updated - on an individual (target or proxy) daemon or the entire cluster. For example:

* Set `stats_time` = 1 minute, `iostat_time` = 4 seconds (scope of the operation: entire **cluster**)
```shell
# curl -i -X PUT 'http://G/v1/cluster/setconfig?stats_time=1m&periodic.iostat_time=4s'
```

> As of v2.0, AIS configuration includes a section called `periodic`. The `periodic` in turn contains several knobs - one of those knobs is `stats_time`, another - `iostat_time`. To update one or both of those named variables on all or one of the clustered nodes, you could:

* Set `stats_time` = 1 minute, `iostat_time` = 4 seconds (scope of the operation: **one AIS node**)
```shell
# curl -i -X PUT 'http://G-or-T/v1/daemon/setconfig?periodic.stats_time=1m&iostat_time=4s'
```

> Notice the **naming convention**: an AIS knob can be referred to by its fully-qualified name: `section-tag.variable-tag`. When there's no ambiguity, the name of the configuration section can be omitted. For instance, `periodic.stats_time` and `stats_time` both reference the same knob and can be used interchangeably.

For more examples and for alternative ways to format configuration-updating requests, please see [examples below](#examples).

Following is a table-summary that contains a *subset* of all *settable* knobs:

| Option | Default value | Description |
|---|---|---|
| log.level | 3 | Set global logging level. The greater number the more verbose log output |
| vmodule | "" | Overrides logging level for a given modules.<br>{"name": "vmodule", "value": "target\*=2"} sets log level to 2 for target modules |
| stats_time | 10s | A node periodically does 'housekeeping': updates internal statistics, remove old logs, and executes extended actions prefetch and LRU waiting in the line |
| dont_evict_time | 120m | LRU does not evict an object which was accessed less than dont_evict_time ago |
| disk_util_low_wm | 60 | Operations that implement self-throttling mechanism, e.g. LRU, do not throttle themselves if disk utilization is below `disk_util_low_wm` |
| disk_util_high_wm | 80 | Operations that implement self-throttling mechanism, e.g. LRU, turn on maximum throttle if disk utilization is higher than `disk_util_high_wm` |
| capacity_upd_time | 10m | Determines how often AIStore updates filesystem usage |
| dest_retry_time | 2m | If a target does not respond within this interval while rebalance is running the target is excluded from rebalance process |
| send_file_time | 5m | Timeout for getting object from neighbor target or for sending an object to the correct target while rebalance is in progress |
| default_timeout | 30s | Default timeout for quick intra-cluster requests, e.g. to get daemon stats |
| default_long_timeout | 30m | Default timeout for long intra-cluster requests, e.g. reading an object from neighbor target while rebalancing |
| lowwm | 75 | If filesystem usage exceeds `highwm` LRU tries to evict objects so the filesystem usage drops to `lowwm` |
| highwm | 90 | LRU starts immediately if a filesystem usage exceeds the value |
| lru.enabled | true | Enables and disabled the LRU |
| rebalance.enabled | true | Enables and disables automatic rebalance after a target receives the updated cluster map. If the(automated rebalancing) option is disabled, you can still use the REST API(`PUT {"action": "rebalance" v1/cluster`) to initiate cluster-wide rebalancing operation |
| cksum.type | xxhash | Hashing algorithm used to check if the local object is corrupted. Value 'none' disables hash sum checking. Possible values are 'xxhash' and 'none' |
| cksum.validate_cold_get | true | Enables and disables checking the hash of received object after downloading it from the cloud or next tier |
| cksum.validate_warm_get | false | If the option is enabled, AIStore checks the object's version (for a Cloud-based bucket), and an object's checksum. If any of the values(checksum and/or version) fail to match, the object is removed from local storage and (automatically) with its Cloud or next AIStore tier based version |
| cksum.enable_read_range | false | Enables and disables checksum calculation for object slices. If enabled, it adds checksum to HTTP response header for the requested object byte range |
| versioning | all | Defines what kind of buckets should use versioning to detect if the object must be redownloaded. Possible values are 'cloud', 'local', and 'all' |
| version.validate_warm_get | false | If false, a target returns a requested object immediately if it is cached. If true, a target fetches object's version(via HEAD request) from Cloud and if the received version mismatches locally cached one, the target redownloads the object and then returns it to a client |
| fshc.enabled | true | Enables and disables filesystem health checker (FSHC) |
| mirror.enabled | false | If true, for every object PUT a target creates object replica on another mountpath. Later, on object GET request, loadbalancer chooses a mountpath with lowest disk utilization and reads the object from it |
| mirror.burst_buffer | 512 | the maximum length of queue of objects to be mirrored. When the queue length exceeds the value, a target may skip creating replicas for new objects |
| mirror.util_thresh | 20 | If mirroring is enabled, loadbalancer chooses an object replica to read but only if main object's mountpath utilization exceeds the replica' s mountpath utilization by this value. Main object's mountpath is the mountpath used to store the object when mirroring is disabled |

## Configuration persistence

By default, configuration updates are transient. To persist the configuration across restarts, use a special knob named `persist`, for instance:

```shell
# curl -i -X PUT 'http://G/v1/cluster/setconfig?stats_time=1m&persist=true'
```

This (above) does two things: updates `stats_time` to 1 minute and stores the updated configuration into local respective locations of all AIS nodes. To *scope* the same request to one specific AIS node, run:

```shell
# curl -i -X PUT 'http://G-or-T/v1/daemon/setconfig?stats_time=1m&persist=true'
```

## Startup override

AIS command-line allows to override (and, optionally, persist) configuration at AIS node's startup. For example:

```shell
# ais -config=/etc/ais.json -role=target -persist=true -confjson="{\"default_timeout\": \"13s\" }"
```

As shown above, the CLI option in-question is: `confjson`. It's value is a JSON-formatted map of string names and string values. You can *persist* the updated configuration either via `-persist` command-line option or via an additional JSON tuple:

```shell
# ais -config=/etc/ais.json -role=target -confjson="{\"default_timeout\": \"13s\", \"persist\": \"true\" }"
```

Another example. To temporarily override locally-configured address of the primary proxy, run:

```shell
# ais -config=/etc/ais.json -role=target -proxyurl=http://G

where G denotes the designated primary's hostname and port.
```

To achieve the same on a more permanent basis, add `-persist=true` as follows:

```shell
# ais -config=/etc/ais.json -role=target -proxyurl=http://G -persist=true
```

> Please see [AIS command-line](command_line.md) for other command-line options and details.

## Managing filesystems

Configuration option `fspaths` specifies the list of local directories where storage targets store objects. An `fspath` aka `mountpath` (both terms are used interchangeably) is, simply, a local directory serviced by a local filesystem.

NOTE: there must be a 1-to-1 relationship between `fspath` and an underlying local filesystem. Note as well that this may be not the case for the development environments where multiple mountpaths are allowed to coexist within a single filesystem (e.g., tmpfs).

AIStore [HTTP API](/docs/http_api.md) makes it possible to list, add, remove, enable, and disable a `fspath` (and, therefore, the corresponding local filesystem) at runtime. Filesystem's health checker (FSHC) monitors the health of all local filesystems: a filesystem that "accumulates" I/O errors will be disabled and taken out, as far as the AIStore built-in mechanism of object distribution. For further details about FSHC, please refer to [FSHC readme](/health/fshc.md).

## Disabling extended attributes

To make sure that AIStore does not utilize xattrs, configure `checksum`=`none` and `versioning`=`none` for all targets in a AIStore cluster. This can be done via the [common configuration "part"](/ais/setup/config.sh) that'd be further used to deploy the cluster.

## Enabling HTTPS

To switch from HTTP protocol to an encrypted HTTPS, configure `use_https`=`true` and modify `server_certificate` and `server_key` values so they point to your OpenSSL certificate and key files respectively (see [AIStore configuration](/ais/setup/config.sh)).

## Filesystem Health Checker

Default installation enables filesystem health checker component called FSHC. FSHC can be also disabled via section "fshc" of the [configuration](/ais/setup/config.sh).

When enabled, FSHC gets notified on every I/O error upon which it performs extensive checks on the corresponding local filesystem. One possible outcome of this health-checking process is that FSHC disables the faulty filesystems leaving the target with one filesystem less to distribute incoming data.

Please see [FSHC readme](/health/fshc.md) for further details.

## Networking

In addition to user-accessible public network, AIStore will optionally make use of the two other networks: internal (or intra-cluster) and replication. If configured via the [net section of the configuration](/ais/setup/config.sh), the intra-cluster network is utilized for latency-sensitive control plane communications including keep-alive and [metasync](/docs/ha.md#metasync). The replication network is used, as the name implies, for a variety of replication workloads.

All the 3 (three) networking options are enumerated [here](/cmn/network.go).

## Reverse proxy

AIStore gateway can act as a reverse proxy vis-à-vis AIStore storage targets. As of the version 2.0, this functionality is limited to GET requests only and must be used with caution and consideration. Related [configuration variable](/ais/setup/config.sh) is called `rproxy` - see sub-section `http` of the section `net`. For further details, please refer to [this readme](/docs/rproxy.md).

## Examples

The following assumes that `G` and `T` are the (hostname:port) of one of the deployed gateways (in a given AIS cluster) and one of the targets, respectively.

#### Cluster-wide operation (all nodes): set the stats logging interval to 1 second
```shell
# curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "1s"}' 'http://G/v1/cluster'

or, same:

# curl -i -X PUT 'http://G/v1/cluster/setconfig?stats_time=1s'
```

> Notice the two alternative ways to form the requests.

#### Cluster-wide operation (all nodes): set the stats logging interval to 2 minutes
```shell
# curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "stats_time", "value": "2m"}' 'http://G/v1/cluster'
```

#### Cluster-wide operation (all nodes): elevate log verbosity to `4` for all sources matching `ais/targ*` regex
```shell
# curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "vmodule", "value": "ais/targ*=4"}' 'http://G/v1/cluster'
```

#### Single-node operation (target at port `Tport`): set log verbosity to `1` for all source files that match the `ais/targ*` regex
```shell
# curl -i -X PUT -H 'Content-Type: application/json' -d '{"action": "setconfig","name": "vmodule", "value": "ais/targ*=1"}' 'http://T/v1/daemon'

or, same:

# curl -i -X PUT 'http://G/v1/daemon/setconfig?vmodule=ais/targ*=1'
```
