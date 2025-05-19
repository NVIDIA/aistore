# AIStore Observability: Prometheus

AIStore (AIS) exposes metrics in [Prometheus](https://prometheus.io) format via HTTP endpoints. This integration enables comprehensive monitoring of AIS clusters, performance tracking, and trend analysis.

## Table of Contents
- [Updates in v3.26](#updates-in-v326)
- [Overview](#overview)
- [Prometheus Exporter](#prometheus-exporter)
- [Viewing Metrics](#viewing-metrics)
- [StatsD Exporter for Prometheus](#statsd-exporter-for-prometheus)
- [References](#references)
- [Related Documentation](#related-documentation)

## Updates in v3.26

* Added [complete reference](31-metrics-reference.md) that includes all supported metric names (both internal and visible externally), their respective types, and descriptions
* Building `aisnode` with StatsD requires the corresponding build tag; build-wise, Prometheus is effectively the default
* For details, see related: [package `stats`](/docs/environment-vars.md#package-stats)

## Overview

AIS tracks a growing list of performance counters, utilization percentages, latency and throughput metrics, transmitted and received stats (total bytes and numbers of objects), error counters, and more.

Full observability is supported using a variety of tools that include:

* AIS node logs
* [CLI](/docs/cli.md), and specifically
* [`ais show cluster stats`](/docs/cli/cluster.md) command

On the monitoring backend side, AIS equally supports:
* [StatsD](https://github.com/etsy/statsd) with any compliant backend (e.g., Graphite/Grafana), and
* [Prometheus](https://prometheus.io/)

The typical monitoring setup with Prometheus looks like this:

```
┌────────────────┐       ┌────────────────┐
│                │ scrape│                │
│   Prometheus   │◄──────┤  AIStore Node  │
│                │       │   /metrics     │
└────────────────┘       └────────────────┘
        │
        │ query
        ▼
┌────────────────┐
│                │
│     Grafana    │
│                │
└────────────────┘
```

> For information on AIS load testing metrics, please refer to [Load Generator](/docs/aisloader.md) and [How To Benchmark AIStore](howto_benchmark.md).

## Prometheus Exporter

AIS is a fully compliant [Prometheus exporter](https://prometheus.io/docs/instrumenting/writing_exporters/) that natively supports [Prometheus](https://prometheus.io/) stats collection. There's no special configuration - the only thing required to enable the corresponding integration is letting AIS know whether to publish its stats via StatsD **or** Prometheus.

> The corresponding binary choice between StatsD and Prometheus is a **build-time** switch that is a single build tag: `statsd`.

> For the complete list of supported build tags, please see [conditional linkage](/docs/build_tags.md).

When a starting-up AIS node (gateway or storage target) is built with Prometheus support (i.e., **without** build tag `statsd`) it will:

* Register all its metric descriptions (names, labels, and helps) with Prometheus, and
* Provide HTTP endpoint `/metrics` for subsequent collection (aka "scraping") by Prometheus.

## Viewing Metrics

One immediate (low-level) way to view supported Prometheus metrics in action would be `curl`:

```console
$ curl http://<aistore-node-ip-or-hostname>:<port>/metrics

## or, possibly:

$ curl https://<aistore-node-ip-or-hostname>:<port>/metrics
```

Here's a few examples of the output:

```console
# HELP ais_target_disk_avg_rsize average read size (bytes)
# TYPE ais_target_disk_avg_rsize gauge
ais_target_disk_avg_rsize{disk="nvme0n1",node_id="ClCt8081"} 4096
# HELP ais_target_disk_avg_wsize average write size (bytes)
# TYPE ais_target_disk_avg_wsize gauge
ais_target_disk_avg_wsize{disk="nvme0n1",node_id="ClCt8081"} 260130
# HELP ais_target_disk_read_mbps read bandwidth (MB/s)
# TYPE ais_target_put_bytes counter
...
ais_target_put_bytes{node_id="ClCt8081"} 1.721761792e+10
# HELP ais_target_put_count total number of executed PUT(object) requests
# TYPE ais_target_put_count counter
ais_target_put_count{node_id="ClCt8081"} 1642
# HELP ais_target_put_ns_total PUT: total cumulative time (nanoseconds)
# TYPE ais_target_put_ns_total counter
ais_target_put_ns_total{node_id="ClCt8081"} 9.44367232e+09
# TYPE ais_target_state_flags gauge
ais_target_state_flags{node_id="ClCt8081"} 6
# HELP ais_target_uptime this node's uptime since its startup (seconds)
# TYPE ais_target_uptime gauge
ais_target_uptime{node_id="ClCt8081"} 210
...
```

For continuous monitoring of any given subset of metrics (still _without_ using actual Prometheus installation) one could also run something like:

```console
for i in {1..99999}; do curl http://hostname:8081/metrics --silent | grep "ais_target_get_n.*node"; sleep 1; done
```

## StatsD Exporter for Prometheus

If, for whatever reason, you decide to use the "StatsD" option, you can still send AIS stats to Prometheus - via its own generic [statsd_exporter](https://github.com/prometheus/statsd_exporter) extension that on-the-fly translates StatsD formatted metrics.

> **Note**: while native Prometheus integration (the previous section) is the preferred and recommended option [statsd_exporter](https://github.com/prometheus/statsd_exporter) can be considered a backup plan for deployments with very special requirements.

First, the picture:

![AIStore monitoring with Prometheus](images/statsd-exporter.png)

The diagram depicts AIS cluster that runs an arbitrary number of nodes with each node periodically sending its StatsD metrics to a configured UDP address of any compliant StatsD server. In fact, [statsd_exporter](https://github.com/prometheus/statsd_exporter) is one such compliant StatsD server that happens to be available out of the box.

To deploy [statsd_exporter](https://github.com/prometheus/statsd_exporter):

* You could either use [prebuilt container image](https://quay.io/repository/prometheus/statsd-exporter);
* Or, `git clone` or `go install` the exporter's own repository at https://github.com/prometheus/statsd_exporter and then run it as shown above. Just take a note of the default StatsD port: **8125**.

To test a combination of AIStore and [statsd_exporter](https://github.com/prometheus/statsd_exporter) without Prometheus, run the exporter with debug:

```console
$ statsd_exporter --statsd.listen-udp localhost:8125 --log.level debug
```

The resulting (debug) output will look something like:

```console
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:321 msg="Starting StatsD -> Prometheus Exporter" version="(version=, branch=, revision=)"
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:322 msg="Build context" context="(go=go1.16.3, user=, date=)"
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:361 msg="Accepting StatsD Traffic" udp=localhost:8125 tcp=:9125 unixgram=
level=info ts=2021-05-13T15:30:22.251Z caller=main.go:362 msg="Accepting Prometheus Requests" addr=:9102
level=debug ts=2021-05-13T15:30:27.811Z caller=listener.go:73 msg="Incoming line" proto=udp line=aistarget.pakftUgh.kalive.latency:1|ms
level=debug ts=2021-05-13T15:30:29.891Z caller=listener.go:73 msg="Incoming line" proto=udp line=aisproxy.qYyhpllR.pst.count:77|c
level=debug ts=2021-05-13T15:30:37.811Z caller=listener.go:73 msg="Incoming line" proto=udp line=aistarget.pakftUgh.kalive.latency:1|ms
level=debug ts=2021-05-13T15:30:39.892Z caller=listener.go:73 msg="Incoming line" proto=udp line=aisproxy.qYyhpllR.pst.count:78|c
level=debug ts=2021-05-13T15:30:47.811Z caller=listener.go:73 msg="Incoming line" proto=udp line=aistarget.pakftUgh.kalive.latency:1|ms
level=debug ts=2021-05-13T15:30:49.892Z caller=listener.go:73 msg="Incoming line" proto=udp line=aisproxy.qYyhpllR.pst.count:79|c
...
```

Finally, point any available Prometheus instance to poll the listening port - **9102** by default - of the exporter.

Note that the two listening ports mentioned - StatsD port **8125** and Prometheus port **9102** - are both configurable via the exporter's command line. To see all supported options, run:

```console
$ statsd_exporter --help
```

## References

* https://prometheus.io/docs/instrumenting/writing_exporters/
* https://prometheus.io/docs/concepts/data_model/
* https://prometheus.io/docs/concepts/metric_types/

## Related Documentation

| Document | Description |
|----------|-------------|
| [Observability: Overview](/docs/00-overview.md) | Introduction to AIS observability approaches |
| [Observability: CLI](/docs/10-cli.md) | Command-line monitoring tools |
| [Observability: Logs](/docs/20-logs.md) | Log-based observability |
| [Observability: Metrics Reference](/docs/31-metrics-reference.md) | Complete metrics catalog |
| [Observability: Grafana](/docs/40-grafana.md) | Visualizing AIS metrics with Grafana |
| [Observability: Kubernetes](/docs/50-k8s.md) | Working with Kubernetes monitoring stacks |
