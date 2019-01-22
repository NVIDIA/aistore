## Table of Contents
- [Metrics with StatsD](#metrics-with-statsd)
    - [Proxy metrics](#proxy-metrics)
    - [Target Metrics](#target-metrics)
    - [Disk Metrics](#disk-metrics)
    - [Keepalive Metrics](#keepalive-metrics)
    - [AIS Loader Metrics](#ais-loader-metrics)

## Metrics with StatsD

In AIStore, each target and proxy communicates with a single [StatsD](https://github.com/etsy/statsd) local daemon listening on a UDP port `8125` (which is currently fixed). If a target or proxy cannot connect to the StatsD daemon at startup, the target (or proxy) will run without StatsD.

StatsD publishes local statistics to a compliant backend service (e.g., [graphite](https://graphite.readthedocs.io/en/latest/)) for easy but powerful stats aggregation and visualization.

Please read more on StatsD [here](https://github.com/etsy/statsd/blob/master/docs/backend.md).

All metric tags (or simply, metrics) are logged using the following pattern:

`prefix.bucket.metric_name.metric_value|metric_type`,

where `prefix` is one of: `aisproxy.<daemon_id>`, `aistarget.<daemon_id>`, or `aisloader.<ip>.<loader_id>` and `metric_type` is `ms` for a timer, `c` for a counter, and `g` for a gauge.

Metrics that AIStore generates are named and grouped as follows:

### Proxy metrics

* `aisproxy.<daemon_id>.get.count.1|c`
* `aisproxy.<daemon_id>.get.latency.<value>|ms`
* `aisproxy.<daemon_id>.put.count.1|c`
* `aisproxy.<daemon_id>.put.latency.<value>|ms`
* `aisproxy.<daemon_id>.delete.count.1|c`
* `aisproxy.<daemon_id>.list.count.1|c`
* `aisproxy.<daemon_id>.list.latency.<value>|ms`
* `aisproxy.<daemon_id>.rename.count.1|c`
* `aisproxy.<daemon_id>.cluster_post.count.1|c`

### Target Metrics

* `aistarget.<daemon_id>.get.count.1|c`
* `aistarget.<daemon_id>.get.latency.<value>|ms`
* `aistarget.<daemon_id>.get.cold.count.1|c`
* `aistarget.<daemon_id>.get.cold.bytesloaded.<value>|c`
* `aistarget.<daemon_id>.get.cold.vchanged.<value>|c`
* `aistarget.<daemon_id>.get.cold.bytesvchanged.<value>|c`
* `aistarget.<daemon_id>.put.count.1|c`
* `aistarget.<daemon_id>.put.latency.<value>|ms`
* `aistarget.<daemon_id>.delete.count.1|c`
* `aistarget.<daemon_id>.list.count.1|c`
* `aistarget.<daemon_id>.list.latency.<value>|ms`
* `aistarget.<daemon_id>.rename.count.1|c`
* `aistarget.<daemon_id>.evict.files.1|c`
* `aistarget.<daemon_id>.evict.bytes.<value>|c`
* `aistarget.<daemon_id>.rebalance.receive.files.1|c`
* `aistarget.<daemon_id>.rebalance.receive.bytes.<value>|c`
* `aistarget.<daemon_id>.rebalance.send.files.1|c`
* `aistarget.<daemon_id>.rebalance.send.bytes.<value>|c`
* `aistarget.<daemon_id>.error.badchecksum.xxhash.count.1|c`
* `aistarget.<daemon_id>.error.badchecksum.xxhash.bytes.<value>|c`
* `aistarget.<daemon_id>.error.badchecksum.md5.count.1|c`
* `aistarget.<daemon_id>.error.badchecksum.md5.bytes.<value>|c`

Example of how these metrics show up in a grafana dashboard:

<img src="images/target-statsd-grafana.png" alt="Target Metrics" width="256">

### Disk Metrics

* `aistarget.<daemon_id>.iostat_*.gauge.<value>|g`

### Keepalive Metrics

* `<prefix>.keepalive.heartbeat.<id>.delta.<value>|g`
* `<prefix>.keepalive.heartbeat.<id>.count.1|c`
* `<prefix>.keepalive.average.<id>.delta.<value>|g`
* `<prefix>.keepalive.average.<id>.count.1|c`
* `<prefix>.keepalive.average.<id>.reset.1|c`

### AIS Loader Metrics

* `aisloader.<ip>.<loader_id>.get.pending.<value>|g`
* `aisloader.<ip>.<loader_id>.get.count.1|c`
* `aisloader.<ip>.<loader_id>.get.latency.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.throughput.<value>|c`
* `aisloader.<ip>.<loader_id>.get.latency.proxyconn.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.proxy.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.targetconn.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.target.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.posthttp.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.proxyheader.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.proxyrequest.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.proxyresponse.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.targetheader.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.targetrequest.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.latency.targetresponse.<value>|ms`
* `aisloader.<ip>.<loader_id>.get.error.1|c`
* `aisloader.<ip>.<loader_id>.put.pending.<value>|g`
* `aisloader.<ip>.<loader_id>.put.count.<value>|g`
* `aisloader.<ip>.<loader_id>.put.latency.<value>|,s`
* `aisloader.<ip>.<loader_id>.put.throughput.<value>|c`
* `aisloader.<ip>.<loader_id>.put.error.1|c`
* `aisloader.<ip>.<loader_id>.getconfig.count.1|c`
* `aisloader.<ip>.<loader_id>.getconfig.latency.<value>|ms`
* `aisloader.<ip>.<loader_id>.getconfig.latency.proxyconn.<value>|ms`
* `aisloader.<ip>.<loader_id>.getconfig.latency.proxy.<value>|ms`
