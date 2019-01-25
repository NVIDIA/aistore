## Table of Contents
- [Configuration](#configuration)
    - [Runtime configuration](#runtime-configuration)
    - [Managing filesystems](#managing-filesystems)
    - [Disabling extended attributes](#disabling-extended-attributes)
    - [Enabling HTTPS](#enabling-https)
    - [Filesystem Health Checker](#filesystem-health-checker)
    - [Networking](#networking)
    - [Reverse proxy](#reverse-proxy)

## Configuration

AIStore configuration is consolidated in a single [JSON file](ais/setup/config.sh) where all of the knobs must be self-explanatory and the majority of those, except maybe just a few, have pre-assigned default values. The notable exceptions include:

<img src="images/ais-config-1.png" alt="Configuration: TCP port and URL" width="600">

and

<img src="images/ais-config-2-commented.png" alt="Configuration: local filesystems" width="600">

As shown above, the `test_fspaths` section of the configuration corresponds to a **single local filesystem being partitioned** between both *local* and *Cloud* buckets. In other words, the `test_fspaths` configuration option is intended strictly for development.

In production we use the an alternative configuration called `fspaths`: the section of the [config](ais/setup/config.sh) that includes a number of local directories, whereby each directory is based on a different local filesystem. 

> Terminology: *mountpath* is a triplet **(local filesystem (LFS), disks that this LFS utilizes, LFS directory)**. The following rules are enforced: 1) different mountpaths use different LFSes, and 2) different LFSes use different disks.

> The terms `fspath` (aka `filesystem path`) and `mountpath` are used interchangeably throughout AIStore docs and sources. When `fspath` configuration is enabled, the 1-to-1 relationship between configured `mountpaths` and local filesystems is enforced and validated at all times.

An example of 12 fspaths (and 12 local filesystems) follows below:

<img src="images/example-12-fspaths-config.png" alt="Example: 12 fspaths" width="160">

### Runtime configuration

Each configuration option can be set on an individual (target | proxy) daemon, by sending a request to the daemon URL (`/v1/daemon`) - or, for the entire cluster, by sending a request to the cluster URL (`/v1/cluster`) of any AIS gateway.

Both a proxy and a storage target support the same set of runtime options but a proxy uses only a few of them. The list of options which affect proxy includes `loglevel`, `vmodule`, `dest_retry_time`, `default_timeout`, and `default_long_timeout`.

| Option | Default value | Description |
|---|---|---|
| loglevel | 3 | Set global logging level. The greater number the more verbose log output |
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
| lru_enabled | true | Enables and disabled the LRU |
| rebalancing_enabled | true | Enables and disables automatic rebalance after a target receives the updated cluster map. If the(automated rebalancing) option is disabled, you can still use the REST API(`PUT {"action": "rebalance" v1/cluster`) to initiate cluster-wide rebalancing operation |
| validate_checksum_cold_get | true | Enables and disables checking the hash of received object after downloading it from the cloud or next tier |
| validate_checksum_warm_get | false | If the option is enabled, AIStore checks the object's version (for a Cloud-based bucket), and an object's checksum. If any of the values(checksum and/or version) fail to match, the object is removed from local storage and (automatically) with its Cloud or next AIStore tier based version |
| checksum | xxhash | Hashing algorithm used to check if the local object is corrupted. Value 'none' disables hash sum checking. Possible values are 'xxhash' and 'none' |
| versioning | all | Defines what kind of buckets should use versioning to detect if the object must be redownloaded. Possible values are 'cloud', 'local', and 'all' |
| fschecker_enabled | true | Enables and disables filesystem health checker (FSHC) |

### Managing filesystems

Configuration option `fspaths` specifies the list of local directories where storage targets store objects. An `fspath` aka `mountpath` (both terms are used interchangeably) is, simply, a local directory serviced by a local filesystem.

NOTE: there must be a 1-to-1 relationship between `fspath` and an underlying local filesystem. Note as well that this may be not the case for the development environments where multiple mountpaths are allowed to coexist within a single filesystem (e.g., tmpfs).

AIStore [HTTP API](docs/http_api.md) makes it possible to list, add, remove, enable, and disable a `fspath` (and, therefore, the corresponding local filesystem) at runtime. Filesystem's health checker (FSHC) monitors the health of all local filesystems: a filesystem that "accumulates" I/O errors will be disabled and taken out, as far as the AIStore built-in mechanism of object distribution. For further details about FSHC, please refer to [FSHC readme](health/fshc.md).

### Disabling extended attributes

To make sure that AIStore does not utilize xattrs, configure `checksum`=`none` and `versioning`=`none` for all targets in a AIStore cluster. This can be done via the [common configuration "part"](ais/setup/config.sh) that'd be further used to deploy the cluster.

### Enabling HTTPS

To switch from HTTP protocol to an encrypted HTTPS, configure `use_https`=`true` and modify `server_certificate` and `server_key` values so they point to your OpenSSL cerificate and key files respectively (see [AIStore configuration](ais/setup/config.sh)).

### Filesystem Health Checker

Default installation enables filesystem health checker component called FSHC. FSHC can be also disabled via section "fschecker" of the [configuration](ais/setup/config.sh).

When enabled, FSHC gets notified on every I/O error upon which it performs extensive checks on the corresponding local filesystem. One possible outcome of this health-checking process is that FSHC disables the faulty filesystems leaving the target with one filesystem less to distribute incoming data.

Please see [FSHC readme](health/fshc.md) for further details.

### Networking

In addition to user-accessible public network, AIStore will optionally make use of the two other networks: internal (or intra-cluster) and replication. If configured via the [netconfig section of the configuration](ais/setup/config.sh), the intra-cluster network is utilized for latency-sensitive control plane communications including keep-alive and [metasync](docs/ha.md#metasync). The replication network is used, as the name implies, for a variety of replication workloads.

All the 3 (three) networking options are enumerated [here](cmn/network.go).

### Reverse proxy

AIStore gateway can act as a reverse proxy vis-à-vis AIStore storage targets. As of the version 2.0, this functionality is limited to GET requests only and must be used with caution and consideration. Related [configuration variable](ais/setup/config.sh) is called `rproxy` - see sub-section `http` of the section `netconfig`. For further details, please refer to [this readme](docs/rproxy.md).
