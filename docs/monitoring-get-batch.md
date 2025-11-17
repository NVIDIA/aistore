## Overview

GetBatch (a.k.a `get-batch` or `x-moss`) is the high-performance multi-object retrieval subsystem.

It streams objects and archived files in strict user-specified order, assembling them on the designated target (DT) and serving them as a TAR archive (buffered or streaming).

> TAR is the default output format but TAR.GZ, ZIP, and TAR.LZ4 are equally supported.

**See:**

* [GetBatch overview and usage](/docs/get_batch.md) for capabilities, operational guidance, and usage (including Go and Python examples).
- [Release Notes v1.4.0](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - GetBatch introduction.

Unlike ordinary GET requests, get-batch:

* pulls objects concurrently from many targets,
* relies on intra-cluster streaming via SharedDM,
* performs archive extraction (for shards),
* and obeys load-based throttling and soft-error recovery.

This page documents how to **observe and monitor** get-batch jobs at scale.

## Key Metrics

All metrics below are **per-target** Prometheus counters/totals.
Use `rate()` or `increase()` over a window for meaningful rates.

### **Workload Volume & Mix**

| Metric               | Description                                                            |
| -------------------- | ---------------------------------------------------------------------- |
| `getbatch.n`         | Total number of get-batch work items processed (successful or failed). |
| `getbatch.obj.n`     | Number of whole objects retrieved and delivered in the output TAR.     |
| `getbatch.file.n`    | Number of files extracted from shard archives.                         |
| `getbatch.obj.size`  | Cumulative size (bytes) of whole objects retrieved.                    |
| `getbatch.file.size` | Cumulative size (bytes) of shard-extracted files.                      |

These represent **actual payload delivered**, not including error placeholders.

---

### Latency, Throttling & Backpressure

| Metric                 | Description                                                                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `getbatch.rxwait.ns`   | Total nanoseconds the DT spent _waiting to receive missing/out-of-order entries_ from peer targets. Reflects SDM/peer/network-induced stalls. |
| `getbatch.throttle.ns` | Total nanoseconds slept due to load-based throttling (memory/cpu pressure). Intentional back-pressure applied before serving _next_ get-batch request. |

> **Interpretation:**

* High **rxwait** → clustering, peer-to-peer streaming, SDM performance, or transient disconnects.
* High **throttle** → DT-level resource pressure (memory load, CPU load, configured Advice).

These two combined provide a full picture of “Why is my job slower?”

---

### Soft vs Hard Errors

| Metric                    | Description                                                                                                               |
| ------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| **`err_soft.getbatch.n`** | Number of soft error events (GFN recoveries and missing-path insertions). A single WI may contribute multiple increments. |
| **`err_getbatch.n`**      | Number of hard errors (unrecoverable WIs, failures including 429 rejections).                                             |

> **Soft errors** reflect recoverable situations;
> **Hard errors** reflect request-level failure or 429 ("too-many-requests") rejection.

---

## PromQL examples

For PromQL examples, please refer to [Observability: Prometheus](/docs/monitoring-prometheus.md) document.

Here, we'll just notice maybe less obvious query that allows to analyze get-batch latency and, in particular, whether the latter is peer-induced vs resource-induced.

## Rx Stall Rate (peer-induced waits)

```promql
rate(ais_target_getbatch_rxwait_ns[5m])
```

## Throttle Stall Rate (load-induced waits)

```promql
rate(ais_target_getbatch_throttle_ns[5m])
```

## Error Behavior

### Soft Error Rate

```promql
rate(ais_target_err_soft_getbatch_n[5m])
```

## Hard Error Rate

```promql
rate(ais_target_err_getbatch_n[5m])
```
