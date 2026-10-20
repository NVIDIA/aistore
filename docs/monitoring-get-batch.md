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

These totals exclude missing-entry placeholders. They measure assembly progress, not guaranteed client-delivered payload: a buffered work item can count an earlier entry and then fail before sending the archive response.

---

### Latency, Throttling & Backpressure

| Internal name | Prometheus name | Description |
| ------------- | --------------- | ----------- |
| `getbatch.rxwait.ns` | `ais_target_getbatch_rxwait_ns` | Total nanoseconds the DT spent _waiting to receive missing/out-of-order entries_ from peer targets. Reflects SDM/peer/network-induced stalls. |
| `getbatch.throttle.ns` | `ais_target_getbatch_throttle_ns` | Total nanoseconds slept due to load-based throttling (memory/cpu pressure). Intentional back-pressure applied before serving _next_ get-batch request. |

> **Interpretation:**

* High **rxwait** → clustering, peer-to-peer streaming, SDM performance, or transient disconnects.
* High **throttle** → DT-level resource pressure (memory load, CPU load, configured Advice).

Together, these metrics measure time spent waiting for peer entries and time spent intentionally throttling. They do not account for every cause of a slower job.

---

### Soft vs Hard Errors

| Internal name | Prometheus name | Description |
| ------------- | --------------- | ----------- |
| `err.soft.getbatch.n` | `ais_target_err_soft_getbatch_count` | Number of soft-error events, including missing objects or archive members represented by missing entries. A work item can contribute multiple increments. |
| `err.getbatch.n` | `ais_target_err_getbatch_count` | Number of hard errors, including request failures and 429 rejections. |

`MaxSoftErrs` is a configured processing guard for accumulated soft-error events; it can stop a work item but does not guarantee that every over-limit event immediately aborts it. With `ContinueOnErr`, missing objects or archive members can be represented as missing entries. The limit is not a retry budget, and a missing entry is not necessarily recoverable. Hard errors include request-level failures and 429 ("too-many-requests") rejections.

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
