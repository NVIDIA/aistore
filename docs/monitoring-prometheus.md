# **AIStore Observability: Prometheus**

AIStore (AIS) exposes metrics in [Prometheus](https://prometheus.io) format via HTTP endpoints.

This integration enables comprehensive monitoring of AIS clusters, performance tracking, capacity planning, and long-term trend analysis.

## Table of Contents

* [Overview](#overview)
* [Monitoring Stack](#monitoring-stack)
* [Prometheus Integration](#prometheus-integration)
  * [Native Exporter](#native-exporter)
  * [Viewing Raw Metrics](#viewing-raw-metrics)
  * [Key Metric Groups](#key-metric-groups)
  * [Metric Labels](#metric-labels)
  * [Essential PromQL Queries](#essential-promql-queries)
  * [GetBatch (x-moss) Queries](#getbatch-x-moss-queries)
* [Node Alerts](#node-alerts)
  * [CLI Monitoring](#cli-monitoring)
  * [Prometheus Queries](#prometheus-queries)
  * [Grafana Alerting](#grafana-alerting)
* [Best Practices](#best-practices)
* [References](#references)
* [Related Documentation](#related-documentation)

---

## Overview

AIS tracks a comprehensive set of performance metrics including:

* Operation counters (GET/PUT/DELETE/etc.)
* Resource utilization (CPU, memory, disk)
* Latencies and throughput
* Network and peer-to-peer streaming statistics
* Extended actions (xactions)
* Error counters and node state

AIS supports observability through several complementary tools:

* Node logs (fine-grained operational events)
* [CLI](/docs/cli.md) for interactive monitoring (e.g., `ais show cluster stats`)
* Monitoring backends:

  * **Prometheus** (recommended)
  * Grafana for dashboards & alerting

> For load testing and benchmarking metrics, see [AIS Load Generator](/docs/aisloader.md) and [How To Benchmark AIStore](/docs/howto_benchmark.md).

A complete catalog of AIS metrics is available at:
**[Monitoring Metrics Reference](/docs/monitoring-metrics.md)**

---

## Monitoring Stack

Typical Prometheus deployment:

```
┌────────────────┐       ┌────────────────┐
│                │ scrape│                │
│   Prometheus   │◄──────┤  AIStore Node  │
│                │       │   /metrics     │
└────────────────┘       └────────────────┘
       ││
       ││ query
       ▼
┌────────────────┐
│                │
│     Grafana    │
│                │
└────────────────┘
```

This stack provides:

* Direct metric collection from AIS nodes
* Centralized metric retention
* Grafana visualization & alerting
* Long-term performance & cost analysis

---

## Prometheus Integration

### Native Exporter

AIS acts as a **first-class Prometheus exporter**. Every node automatically:

1. Registers all metrics at startup
2. Exposes `/metrics` for Prometheus to scrape
3. Uses Prometheus native formatting and metadata
4. Works with both HTTP and HTTPS clusters

> No configuration is required to “enable” Prometheus — it is always on.

AIS source metrics (`put.size`, `get.ns`, etc.) are exported with AIS naming conventions:

```
ais_target_<metric_name>{node_id="T1"} <value>
```

This document primarily uses the exported Prometheus names.

---

### Viewing Raw Metrics

View metrics directly:

```bash
$ curl http://<node>:<port>/metrics
# or
$ curl https://<node>:<port>/metrics
```

Example:

```
# HELP ais_target_put_bytes total bytes served via PUT
# TYPE ais_target_put_bytes counter
ais_target_put_bytes{node_id="ClCt8081"} 1.721761792e+10

# HELP ais_target_put_ns_total total PUT latency (nanoseconds)
# TYPE ais_target_put_ns_total counter
ais_target_put_ns_total{node_id="ClCt8081"} 9.44367232e+09

# HELP ais_target_state_flags node state and alert flags
# TYPE ais_target_state_flags gauge
ais_target_state_flags{node_id="ClCt8081"} 6
```

To watch GET rates without Prometheus:

```bash
for i in {1..99999}; do
  curl -s http://hostname:8081/metrics | grep "ais_target_get_count"
  sleep 1
done
```

---

### Key Metric Groups

AIS organizes metrics into **four major groups**, reflected in the codebase and Prometheus exporter:

| Group                              | Description                                                                            | Examples                                                                       |
| ---------------------------------- | -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| **1. Datapath**                    | GET/PUT counters, sizes, latencies, rate-limiting, I/O errors                          | `ais_target_get_count`, `ais_target_put_bps`, `ais_target_ratelim_retry_get_n` |
| **2. Metadata (in-memory)**        | Lcache activity (evictions, collisions)                                                | `ais_target_lcache_evicted_count`                                              |
| **3. Extended Actions (xactions)** | Background & multi-object jobs: LRU, EC, rebalance, ETL, Download, DSort, **GetBatch** | `ais_target_lru_evict_n`, `ais_target_getbatch_n`                              |
| **4. Streams**                     | Long-lived peer-to-peer (SharedDM) streaming channels                                  | `ais_target_streams_out_obj_n`                                                 |

> For GetBatch observability, see
> **[Monitoring GetBatch](/docs/monitoring-get-batch.md)**.

---

### Metric Labels

AIS exposes labels for filtering and aggregation:

| Label      | Usage                                    |
| ---------- | ---------------------------------------- |
| `node_id`  | Node identity (target or gateway)        |
| `disk`     | Disk name for per-disk metrics           |
| `bucket`   | Source/destination bucket                |
| `xaction`  | Xaction UUID for multi-object jobs       |
| `slice`    | For erasure coding slice metrics         |
| `archpath` | For per-file shard extraction (GetBatch) |

Labels enable PromQL queries such as:

```
sum by (node_id)(rate(ais_target_put_bytes[5m]))
sum by (disk)(ais_target_disk_util)
```

---

## Essential PromQL Queries

### GET operations per second

```promql
sum(rate(ais_target_get_count[5m]))
```

### Average GET latency (ms)

```promql
sum(rate(ais_target_get_ns_total[5m]))
/ sum(rate(ais_target_get_count[5m]))
/ 1e6   # convert ns → ms
```

### Disk utilization

```promql
ais_target_disk_util{disk="nvme0n1"}
```

### GET error percentage

```promql
sum(rate(ais_target_err_get_count[5m]))
/ sum(rate(ais_target_get_count[5m])) * 100
```

### Total cluster capacity usage

```promql
sum(ais_target_capacity_used)
/
sum(ais_target_capacity_total)
* 100
```

---

## GetBatch (x-moss) Queries

GetBatch is AIStore’s high-performance multi-object retrieval pipeline.
Metrics describe throughput, composition (objects vs files), Rx stalls, throttling, and error behavior.

### Work items per second

```promql
sum(rate(ais_target_getbatch_n[5m]))
```

### Logical payload throughput

```promql
sum(rate(ais_target_getbatch_obj_size[5m]))
+
sum(rate(ais_target_getbatch_file_size[5m]))
```

### Stall breakdown (RxWait vs Throttle)

```promql
sum(rate(ais_target_getbatch_rxwait_ns[5m]))
/
(
  sum(rate(ais_target_getbatch_rxwait_ns[5m])) +
  sum(rate(ais_target_getbatch_throttle_ns[5m]))
)
```

### Soft vs Hard Error Rates

```promql
rate(ais_target_err_soft_getbatch_n[5m])
rate(ais_target_err_getbatch_n[5m])
```

Full details and operational guidance:
**→ [Monitoring GetBatch](/docs/monitoring-get-batch.md)**

---

## Node Alerts

AIS nodes expose operational alerts and states via `ais_target_state_flags`.
Flags indicate:

### Red (critical)

* `OOS` — Out of space
* `OOM` — Out of memory
* `DiskFault` — Disk failures
* `NumGoroutines` — excessive goroutines
* `CertificateExpired` — TLS expiry
* `KeepAliveErrors` — peer connectivity issues

### Warning

* `Rebalancing`, `Resilvering`
* `RebalanceInterrupted`, `ResilverInterrupted`
* `LowCapacity`, `LowMemory`, `LowCPU`
* `NodeRestarted`
* `MaintenanceMode`
* `CertWillSoonExpire`

### Informational

* `ClusterStarted`
* `NodeStarted`
* `VoteInProgress`

### CLI Monitoring

```bash
$ ais show cluster
```

### Prometheus Queries

Critical:

```promql
ais_target_state_flags & 8192  > 0  # OOS
or ais_target_state_flags & 16384 > 0  # OOM
or ais_target_state_flags & 65536 > 0  # DiskFault
```

Warnings:

```promql
ais_target_state_flags & 4096 > 0  # LowCapacity
or ais_target_state_flags & 8192 > 0  # LowMemory
```

### Grafana Alert Example

```
ais_target_state_flags{node_id=~"$node"} & 8192 > 0
```

---

## Best Practices

1. **Prometheus retention**
   Plan retention around performance analysis needs (14–30 days recommended).

2. **Dashboard segmentation**
   Maintain dashboards for:

   * Cluster overview
   * Node-level performance
   * Resource utilization
   * Error monitoring
   * Extended actions (GetBatch, rebalance, ETL)

3. **Alerts on critical states**
   Monitor:

   * Node state flags
   * Error spikes
   * Disk utilization
   * High throttle or rxwait stalls (GetBatch)

4. **Scrape frequency**
   5–15 seconds for critical workloads; 30s+ for low-traffic clusters.

---

## Related Documentation

| Document                                              | Description                                 |
| ----------------------------------------------------- | ------------------------------------------- |
| [Overview](/docs/monitoring-overview.md)              | AIS observability introduction              |
| [CLI](/docs/monitoring-cli.md)                        | CLI monitoring and commands                 |
| [Logs](/docs/monitoring-logs.md)                      | Log-based observability                     |
| [Metrics Reference](/docs/monitoring-metrics.md)      | Full AIS metric catalog                     |
| [Grafana](/docs/monitoring-grafana.md)                | Grafana dashboards                          |
| [Kubernetes](/docs/monitoring-kubernetes.md)          | K8s deployment monitoring                   |
| [GetBatch Monitoring](/docs/monitoring-get-batch.md)  | Multi-object retrieval metrics and analysis |

Separately, Prometheus references:

* [Prometheus Exporters](https://prometheus.io/docs/instrumenting/writing_exporters/)
* [Prometheus Data Model](https://prometheus.io/docs/concepts/data_model/)
* [Prometheus Metric Types](https://prometheus.io/docs/concepts/metric_types/)

