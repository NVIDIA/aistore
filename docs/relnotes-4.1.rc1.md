# AIStore v4.1 RC1 Release Notes

## Overview

AIStore v4.1 extends the functionality introduced in v4.0 with a set of changes aimed at improving multi-object retrieval for ML workloads, strengthening authentication, and unifying system behavior under load. Several subsystems were restructured for clarity, consistency, and better operational characteristics. Configuration files and client tooling were updated accordingly.

---

**Table of Contents**

1. [GetBatch: Distributed Multi-Object Retrieval](#getbatch-distributed-multi-object-retrieval)
2. [Authentication and Security](#authentication-and-security)
3. [Chunked Objects](#chunked-objects)
4. [Rechunk Job](#rechunk-job)
5. [Unified Load and Throttling](#unified-load-and-throttling)
6. [Transport Layer](#transport-layer)
7. [Backend Updates](#backend-updates)
8. [Python SDK](#python-sdk)
9. [Build System and Tooling](#build-system-and-tooling)
10. [Xaction Lifecycle](#xaction-lifecycle)
11. [ETL and Transform Pipeline](#etl-and-transform-pipeline)
12. [Observability](#observability)
13. [Configuration Changes](#configuration-changes)

---

## GetBatch: Distributed Multi-Object Retrieval

The GetBatch API (ML endpoint) now has a more complete implementation across the cluster. Retrieval is streaming-oriented, supports multi-bucket batches, and includes tunable soft-error handling. The request path incorporates load-based throttling and can return HTTP 429 when the system is under pressure. Memory and disk pressure are taken into account, and connection resets or route changes are handled transparently.

Configuration is exposed via a new `get_batch` section:

```json
{
  "max_wait": "30s",           // Wait time for remote targets (range: 1s-1m)
  "warmup_workers": 2,         // Pagecache read-ahead workers (-1=disabled, 0-10)
  "max_soft_errs": 6           // Recoverable error limit per request
}
```

Observability has improved through consolidated counters, Prometheus metrics, and clearer status reporting.
Client and tooling updates include a new `Batch` API in the Python SDK, extended aisloader support, and ansible composer playbooks for distributed benchmarks.

> Reference: [https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md](https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md)

---

## Authentication and Security

The authentication configuration has been reorganized but remains backward compatible with the v4.0 format. The new structure separates signature parameters, required claims, and OIDC issuer handling. Token verification supports both symmetric and asymmetric algorithms and validates standard JWT fields such as `aud`.

The v4.0 format continues to be accepted:

```json
{
  "auth": {
    "enabled": true,
    "secret": "your-hmac-secret"
  }
}
```

The new v4.1 structure introduces more explicit fields:

```json
{
  "auth": {
    "enabled": true,
    "signature": {
      "key": "your-key",
      "method": "HS256"
    },
    "required_claims": {
      "aud": ["your-audience"]
    },
    "oidc": {
      "allowed_iss": ["https://your-issuer.com"],
      "issuer_ca_bundle": "/path/to/ca.pem"
    }
  }
}
```

Internal OIDC handling includes JWKS caching, issuer validation, and optional custom CA bundles. S3 client calls that rely on STS-style authentication can now interoperate with JWTs where applicable. Internally, the token cache has been sharded to reduce contention.

### Cluster-Key Authentication

A new cluster-internal authentication mechanism supports signing redirect URLs with an HMAC shared key distributed via metasync. Key rotation, nonce validation, and limited grace periods are configurable:

```json
{
  "auth": {
    "cluster_key": {
      "enabled": true,
      "ttl": "24h",              // 0 = never expire, min: 1h
      "nonce_window": "1m",      // Clock skew tolerance (max: 10m)
      "rotation_grace": "1m"     // Accept old+new key during rotation (max: 1h)
    }
  }
}
```

---

## Chunked Objects

The chunked-object subsystem adds a new hard limit on maximum monolithic object size. This prevents ingestion of extremely large single-object payloads that exceed the cluster’s capacity to process them. The existing soft `objsize_limit` must fit within the hard limit.

```json
{
  "chunks": {
    "objsize_limit": "100GiB",
    "max_monolithic_size": "1TiB",
    "chunk_size": "1GiB",
    "checkpoint_every": 128
  }
}
```

The `max_monolithic_size` value is validated against a fixed range (1 GiB–1 TiB). Auto-chunking for cold GETs is now controlled by bucket properties and integrated with blob downloader behavior. The manifest format introduced in v4.0 continues to be used.

---

## Rechunk Job

A new job (“rechunk”) rewrites existing objects using updated chunk parameters. This is useful when bucket-level chunking policies change or when an existing dataset must be restructured for more predictable retrieval patterns.

---

## Unified Load and Throttling

A new package (`cmn/load`) unifies load evaluation across the cluster. The load vector combines memory usage, CPU load, disk utilization, network activity, and file-descriptor pressure. Subsystems that previously used independent throttling heuristics now rely on a shared advisory mechanism.

The unified logic is used by GetBatch, blob downloader, the transport layer, and general xaction control. This results in consistent backpressure signals and more predictable behavior during high load.

---

## Transport Layer

Transport behavior has been clarified and refactored. Stream reconnection is more explicit, and all error paths now use typed errors with consistent formatting. Protocol violations, EOF, and retriable network errors are distinguished more cleanly.

Overhead has been reduced by eliminating per-connection stats and tightening stream construction. Connection-drop scenarios now propagate clearer failure signals to callers.

---

## Backend Updates

Multipart upload support is now implemented consistently across Azure and GCP backends. Other backend improvements include corrected range-read size reporting for GCP and adherence to S3’s UTC `Last-Modified` format. The set of provider behaviors is now more uniform across cloud backends.

---

## Python SDK

Python SDK v1.17 includes support for the updated ML endpoint, Pydantic v2, and Python 3.14. The new `Batch` API replaces older multi-object loaders. Authentication test coverage has been revised, and retry behavior for 429 throttling has been improved. Additional examples and archive workflows have been added.

---

## Build System and Tooling

AIStore has migrated to Go 1.25. Dependencies were refreshed, and new linters such as `modernize` have been enabled. Several internal utilities were replaced with standard library equivalents (`slices.Contains`, etc.). Struct field alignment and generic patterns were cleaned up.

`aisloader` received expanded archive and shard workload support, percentage-based sampling, improved name-getters using affine/prime distributions, and better memory reuse. These updates make it easier to run controlled benchmarks for GetBatch and shard-based datasets.

---

## Xaction Lifecycle

Xaction lifecycle handling has been made more uniform. The stop/done transition is clearer, and `Snap()` structures are generated consistently. Renewal logic in `core/xreg` has been updated to avoid stale entries. These changes improve behavior for long-running or shared-stream xactions such as GetBatch.

---

## ETL and Transform Pipeline

The ETL framework for Go-based transforms includes improved connection reuse for Python ETLs and more efficient handling of CPU-bound transformations. These improvements reduce overhead in both streaming and offline transform paths.

---

## Observability

Prometheus metrics now cover GetBatch behavior, blob downloads, transport streams, and xaction progress. Logging has been standardized across several components and provides clearer diagnostics and size formatting.

---

## Configuration Changes

### New Sections

* `get_batch`: multi-object retrieval settings

### Modified Sections

* `auth`: new signature/claims/OIDC/cluster-key structure
* `chunks`: new `max_monolithic_size` limit
* `keepalive`: refined interval and detection validation

### Compatibility

* v4.0 auth configurations are automatically migrated.
