## Overview

GetBatch (a.k.a `get-batch` or `x-moss`) is the high-performance multi-object retrieval subsystem.

> **Paper:** [GetBatch: Distributed Multi-Object Retrieval for ML Data Loading](https://arxiv.org/abs/2602.22434) - implementation, benchmarks, and discussion.

It streams objects and archived files in strict user-specified order, assembling them on the designated target (DT) and serving them as a TAR archive (buffered or streaming).

> TAR is the default output format; compressed serialized options include TAR.GZ, ZIP, and TAR.LZ4 and are also fully supported.

**See also:**

* [GetBatch: Multi-Object Retrieval API](/docs/get_batch.md) for overview, capabilities and APIs, operational guidance, usage, Go and Python examples, and more.
- [Release Notes 4.0](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - GetBatch introduction.

Unlike ordinary GET requests, get-batch:

* pulls objects concurrently from many targets,
* relies on intra-cluster streaming via SharedDM,
* performs archive extraction (for shards),
* and obeys load-based throttling and a limit on tolerated soft-error events.

This page documents how to **observe and monitor** get-batch jobs at scale.

## Key Metrics

The tables pair internal metric names with exported per-target Prometheus names. Counters ending in `.n` become `_count`, sizes ending in `.size` become `_bytes`, and periods in cumulative `.ns` totals become underscores. Use `rate()` or `increase()` over a window for meaningful rates.

### **Workload Volume & Mix**

| Internal name | Prometheus name | Description |
| ------------- | --------------- | ----------- |
| `getbatch.n` | `ais_target_getbatch_count` | Total number of get-batch work items processed (successful or failed). |
| `getbatch.obj.n` | `ais_target_getbatch_obj_count` | Number of successfully assembled whole-object entries, including zero-size objects. |
| `getbatch.file.n` | `ais_target_getbatch_file_count` | Number of successfully assembled files extracted from shard archives, including zero-size files. |
| `getbatch.obj.size` | `ais_target_getbatch_obj_bytes` | Cumulative logical bytes of successfully assembled whole-object entries. |
| `getbatch.file.size` | `ais_target_getbatch_file_bytes` | Cumulative logical bytes of successfully assembled shard-extracted files. |

Missing-entry placeholders are not counted.

---

### Latency, Throttling & Backpressure

| Internal name | Prometheus name | Description |
| ------------- | --------------- | ----------- |
| `getbatch.rxwait.ns` | `ais_target_getbatch_rxwait_ns` | Total nanoseconds the DT spent _waiting to receive missing/out-of-order entries_ from peer targets. Reflects SDM/peer/network-induced stalls. |
| `getbatch.throttle.ns` | `ais_target_getbatch_throttle_ns` | Total nanoseconds slept due to load-based throttling (memory/cpu pressure). Intentional back-pressure applied before serving _next_ get-batch request. |

> **Interpretation:**

* High **rxwait** → clustering, peer-to-peer streaming, SDM performance, or transient disconnects.
* High **throttle** → DT-level resource pressure (memory load, CPU load, configured Advice).

Together, these two metrics separate time spent waiting on peers from time spent intentionally throttling.

---

### Soft vs Hard Errors

| Internal name | Prometheus name | Description |
| ------------- | --------------- | ----------- |
| `err.soft.getbatch.n` | `ais_target_err_soft_getbatch_count` | Number of soft errors: missing objects or archived files, stream breaks, and GFN (get-from-neighbor) recoveries. A single work item may contribute multiple increments. |
| `err.getbatch.n` | `ais_target_err_getbatch_count` | Number of hard errors, including request failures and 429 rejections. |

With `coer` (continue-on-error) set in the request, missing objects and archived files are included in the output as empty placeholders under the `__404__/` prefix, and the request keeps going. The per-request number of soft errors is limited by `get_batch.max_soft_errs`; once exceeded, the work item fails.

Hard errors reflect request-level failures, including 429 ("too-many-requests") rejections.

---

## PromQL examples

For PromQL examples, please refer to [Observability: Prometheus](/docs/monitoring-prometheus.md) document.

In this section, we only highlight a few less obvious queries that may help analyze GetBatch latency - in particular, distinguish peer-induced stalls from resource-induced throttling.

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
rate(ais_target_err_soft_getbatch_count[5m])
```

## Hard Error Rate

```promql
rate(ais_target_err_getbatch_count[5m])
```
