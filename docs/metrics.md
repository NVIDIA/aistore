---
layout: post
title: METRICS
permalink: /docs/metrics
redirect_from:
 - /metrics.md/
 - /docs/metrics.md/
---

**UPDATE**: to build `aisnode` with support for StatsD, use the namesake build tag: `statsd`. Note that Prometheus is the default supported option.

> See also: [conditional linkage](/docs/build_tags.md)

**UPDATE**: for [complete reference](/docs/metrics-reference.md) that includes all supported metric names (both internal and visible externally), their respective types, descriptions, and labels, please refer to:

* [Reference: all supported metrics](/docs/metrics-reference.md)

As far Prometheus, one immediate (low-level) way to view the metrics in action would be `curl`:

```console
$ curl http://<aistore-node-ip-or-hostname>:<port>/metrics

## or, possibly:

$ curl https://<aistore-node-ip-or-hostname>:<port>/metrics
```

**UPDATE**: rest of this document may contain a slightly outdated information.

===================

AIStore tracks, logs, and reports a large and growing number of counters, latencies and throughputs including (but not limited to) metrics that reflect cluster recovery and global rebalancing, all [extended long-running operations](https://github.com/NVIDIA/aistore/blob/main/xact/README.md), and, of course, the basic read, write, list transactions and more.

A somewhat outdated example of how these metrics show up in the Grafana dashboard follows:

![AIS loader metrics](images/aisloader-statsd-grafana.png)

But generally, complete observability is supported with a variety of tools, including:

1. System logs
2. [CLI](/docs/cli.md) and, in particular, [`ais show performance`](/docs/cli/performance.md) command
3. [Prometheus](/docs/prometheus.md)
4. Any [StatsD](https://github.com/etsy/statsd) compliant [backend](https://github.com/statsd/statsd/blob/master/docs/backend.md#supported-backends) including Graphite/Grafana

> AIStore includes `aisloader` - a powerful tool that we use to simulate a variety of AI workloads. For numerous command-line options and usage examples, please see [`aisloader`](/docs/aisloader.md) and [How To Benchmark AIStore](/docs/howto_benchmark.md).

> Or, just run `make aisloader; aisloader` and see its detailed online help. Note as well that `aisloader` is fully StatsD-enabled and supports detailed protocol-level tracing with runtime on and off switching.

## StatsD and Prometheus

AIStore generates a growing number of detailed performance metrics. Other than AIS logs, the stats can be viewed via:

* StatsD/Grafana visualization
or
* Prometheus visualization

> [StatsD](https://github.com/etsy/statsd) publishes local statistics to a compliant backend service (e.g., [Graphite](https://graphite.readthedocs.io/en/latest/)) for easy and powerful stats aggregation and visualization.

> AIStore is a fully compliant [Prometheus exporter](https://prometheus.io/docs/instrumenting/writing_exporters/) that natively supports [Prometheus](https://prometheus.io/) stats collection. There's no special configuration - the only thing required to enable the corresponding integration is letting AIStore know whether to publish its stats via StatsD **or** Prometheus.

The StatsD/Grafana option imposes a certain easy-to-meet requirement on the AIStore deployment. Namely, it requires that StatsD daemon (aka service) is **deployed locally with each AIS target and with each AIS proxy**.

At startup AIStore daemons, both targets and gateways, try to UDP-ping their respective local [StatsD](https://github.com/etsy/statsd) daemons on the UDP port `8125` unless redefined via environment `AIS_STATSD_PORT`. You can disable StatsD reachability probing by setting another environment variable - `AIS_STATSD_PROBE` - to `false` or `no`.

If StatsD server is *not* listening on the local 8125, the local AIS target (or proxy) will then run without StatsD, and the corresponding stats won't be captured and won't be visualized.

> For details on all StatsD-supported backends, please refer to [this document](https://github.com/etsy/statsd/blob/master/docs/backend.md).

> For Prometheus integration, please refer to [this separate document](/docs/prometheus.md)

