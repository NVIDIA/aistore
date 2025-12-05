# AIStore 4.1 Release Notes

## Overview

AIStore v4.1 extends the functionality introduced in v4.0 with a set of changes aimed at improving multi-object retrieval for ML workloads, strengthening authentication, and unifying system behavior under load. Several subsystems were restructured for clarity, consistency, and better operational characteristics. Configuration files and client tooling were updated accordingly.

---

**Table of Contents**

1. [GetBatch: Distributed Multi-Object Retrieval](#getbatch-distributed-multi-object-retrieval)
2. [Authentication and Security](#authentication-and-security)
3. [Chunked Objects](#chunked-objects)
4. [Blob Downloader](#blob-downloader)
5. [Rechunk Job](#rechunk-job)
6. [Unified Load and Throttling](#unified-load-and-throttling)
7. [Transport Layer](#transport-layer)
8. [Multipart Upload](#multipart-upload)
9. [Python SDK](#python-sdk)
10. [S3 Compatibility](#s3-compatibility)
11. [Build System and Tooling](#build-system-and-tooling)
12. [Xaction Lifecycle](#xaction-lifecycle)
13. [ETL and Transform Pipeline](#etl-and-transform-pipeline)
14. [Observability](#observability)
15. [Configuration Changes](#configuration-changes)
16. [Tools: `aisloader`](#tools-aisloader)

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

AIS v4.1 introduces a standardized JWT validation model, reorganized configuration for external authentication, and an expanded cluster-key mechanism for securing intra-cluster redirects. Together, these changes provide clearer semantics, better interoperability with third-party identity providers, and more uniform behavior across proxies and targets.

### JWT Validation Model and Token Requirements

AIS uses JWTs to both authenticate and authorize requests. Version 4.1 formalizes this process and documents the complete validation flow in [`auth_validation.md`](https://github.com/NVIDIA/aistore/blob/main/docs/auth_validation.md).

Tokens may be issued by the first-party AuthN service or by compatible third-party identity providers (Keycloak, Auth0, custom OAuth services). AIS makes authorization decisions directly from JWT claims; no external role lookups are performed during request execution.

When `auth.enabled=true` is set in the cluster configuration, proxies validate tokens before routing requests to targets; targets verify redirect signatures when cluster-key signing is enabled.

#### Token Requirements

AIS accepts tokens signed with supported HMAC or RSA algorithms (`HS256/384/512`, `RS256/384/512`).
All tokens must include the standard `sub` and `exp` claims; `aud` and `iss` are validated when required by configuration.

AIS also recognizes several AIS-specific claims:

- `admin`: Full administrative access; overrides all other claims
- `clusters`: Cluster-scoped permissions (specific cluster UUID or wildcard)
- `buckets`: Bucket-scoped permissions tied to individual buckets within a cluster

Cluster and bucket permissions use the access-flag bitmask defined in `api/apc/access.go`.

#### Signature Verification: Static or OIDC

AIS supports two mutually exclusive approaches for signature verification:

1. **Static verification (`auth.signature`)**
   - HMAC (shared secret) or RSA public-key–based
   - Suitable for the AIS AuthN service or controlled token issuers
   - Verifies tokens using the configured secret or public key

2. **OIDC verification (`auth.oidc`)**
   - Automatic discovery using `/.well-known/openid-configuration`
   - JWKS retrieval and caching with periodic refresh
   - Validates issuer (`iss`) against `allowed_iss`
   - Supports custom CA bundles for TLS verification

Both modes accept standard `Authorization: Bearer <token>` headers and AWS-compatible `X-Amz-Security-Token`.

#### Authentication Flow

1. Extract token from `Authorization` or `X-Amz-Security-Token`.
2. Validate signature via static credentials or OIDC discovery.
3. Check standard claims (`sub`, `exp`, and optionally `aud`, `iss`).
4. Evaluate AIS-specific claims (`admin`, `clusters`, `buckets`) to authorize the operation.
5. If cluster-key signing is enabled, sign redirect URLs before forwarding to a target; targets verify signatures prior to execution.

This flow applies to all AIS APIs, including S3-compatible requests.

### Configuration Changes and Compatibility (v4.0 => v4.1)

The authentication configuration has been reorganized for clarity in v4.1, but the previous format remains fully supported:

```json
{
  "auth": {
    "enabled": true,
    "secret": "your-hmac-secret"
  }
}
````

Version 4.1 introduces explicit sections for signature verification, required claims, and OIDC issuer configuration:

```json
{
  "auth": {
    "enabled": true,
    "signature": {
      "key": "your-key",
      "method": "HS256"  // or RS256, RS384, RS512
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

OIDC handling includes JWKS caching, issuer validation, and optional CA bundles.
Token-cache sharding reduces lock contention under heavy concurrency.

See the **JWT Validation Model and Token Requirements** subsection above for validation flow and claim semantics.

### Cluster-Key Authentication

Cluster-key authentication provides HMAC-based signing for internal redirect URLs.
It is independent from user JWT authentication and ensures that targets accept only authenticated, correctly routed internal redirects.

```json
{
  "auth": {
    "cluster_key": {
      "enabled": true,
      "ttl": "24h",              // 0 = never expire, min: 1h
      "nonce_window": "1m",      // Clock-skew tolerance (max: 10m)
      "rotation_grace": "1m"     // Accept old+new key during rotation (max: 1h)
    }
  }
}
```

When enabled, the primary proxy generates a versioned secret and distributes it via metasync.
Proxies sign redirect URLs after validating the caller’s token; targets verify the signature before performing redirected operations.
The mechanism enforces correct routing, provides defense-in-depth against forged redirect traffic, and integrates with timestamp and nonce validation.

---

## Blob Downloader

The [blob downloader](https://github.com/NVIDIA/aistore/blob/main/docs/blob_downloader.md) has been extended to use chunked object representation for faster remote object retrieval. Large objects are split into smaller chunks and fetched via concurrent range-reads. Each chunk is streamed directly to a separate local file, bypassing intermediate buffering and aggregating write bandwidth across all available disks on the target.

The downloader is integrated with the unified load system and adjusts its behavior dynamically. When memory or disk pressure rises, chunk sizes are recalculated and workers may back off to avoid overload. When the cluster has headroom, downloads run at full speed.

[Benchmarks](https://aistore.nvidia.com/blog/2025/11/26/blob-downloader) show a 4x speedup for 4GiB objects compared to standard cold-GET and a 2.28x improvement for prefetch operations on 1.56TiB buckets.

The blob downloader is accessible through three interfaces:

1. **Direct blob-download job** for specific objects
2. **Prefetch integration** with configurable `--blob-threshold` parameter
3. **Streaming GET** that caches while streaming to client

Configuration parameters include chunk size and worker count, with automatic throttling when the cluster is under load.

> [Blob Downloader Blog Post](https://aistore.nvidia.com/blog/2025/11/26/blob-downloader)

---

## Rechunk Job

AIS v4.1 introduces a new “rechunk” xaction that converts existing objects between monolithic and chunked representations based on updated bucket-level chunking policies.

The job rewrites objects in place according to the specified threshold and chunk size, allowing datasets to be reorganized without re-uploading data.

Rechunking is useful when:

- bucket chunking parameters (`objsize_limit`, `chunk_size`) change (see example below);
- a dataset originally stored as monolithic objects must be reshaped for faster ML-oriented access patterns;
- an existing chunked dataset must be normalized to new chunk sizes;
- auto-chunking behavior is enabled or disabled for cold GETs or blob-downloader workflows.

Here's a default bucket configuration (that can be updated at any time):

```console
$ ais bucket props show BUCKET chunks --json
{
    "chunks": {
        "objsize_limit": "0B",
        "max_monolithic_size": "1TiB",
        "chunk_size": "1GiB"
    }
}
```

### Behavior

The rechunk job processes all (or prefix-filtered) objects in a bucket and rewrites them according to the active or explicitly provided chunking configuration:

- Objects **below** `objsize_limit` => rewritten as **monolithic**
- Objects **at or above** `objsize_limit` => rewritten into **chunks** of `chunk_size`
- Setting `objsize_limit=0` restores all objects as monolithic
- Regardless of configuration, objects larger than the bucket’s `max_monolithic_size` are always chunked for correctness and manageability

Rechunking preserves object content and metadata; only the physical storage layout changes.
The job emits standard xaction progress snapshots and integrates with unified load and throttling.

### Usage

```console
$ ais rechunk BUCKET [--chunk-size SIZE] [--objsize-limit SIZE] [--prefix PREFIX]
```

* Chunking parameters may be provided explicitly or derived from current bucket properties.
* When only one of the two size parameters is provided, AIS prompts for confirmation before starting the job.
* Prefix filters can be embedded directly in the bucket URI (e.g.: `ais://bucket/prefix/` or `gs://bucket/prefix`, etc.).

### Operational Notes

* Rechunking can be run online; objects remain readable throughout the operation.
* The job honors cluster load advisories and backs off under pressure.
* Only the storage representation changes; object versions, checksums, and names remain intact.
* Rechunk integrates with the chunk-manifest (ufest) format introduced in v4.0.
* Rechunked objects immediately benefit from GetBatch and blob-downloader optimizations.

---

## Unified Load and Throttling

AIS v4.1 introduces a unified load-evaluation subsystem used across the cluster to provide consistent backpressure when the system becomes resource-constrained. Previously, individual components implemented their own heuristics. The new `cmn/load` package provides a single mechanism for assessing system pressure and generating throttling recommendation for callers.

### Five-Dimensional Load Vector

System load is now evaluated along five independent dimensions:

- Memory pressure
- CPU load averages
- Goroutine count
- Disk utilization
- File-descriptor usage (reserved for future use)

Each dimension is graded from **Low → Moderate → High → Critical**, and the highest observed level influences throttling behavior. Memory pressure has the highest priority; critical memory immediately triggers aggressive back-off.

### Throttling Advice

Subsystems request a `load.Advice` object that determines:

- **How long to sleep** (if throttling is necessary)
- **How frequently to check** current load
- **The highest pressure dimension** observed

Stateful `load.Advice` adapts sampling frequency and back-off automatically. Data-heavy operations (GET, PUT, EC encoding, mirroring, GetBatch, chunked I/O, blob downloader) slow down more aggressively than metadata-only operations (LRU eviction, space cleanup, storage summaries).

The load system is now used across codebase, including:

- GetBatch (that may throttle itself or return **HTTP 429** when OOM)
- Blob downloader (ditto)
- Rechunk; other long-running xactions (jobs)
- EC and mirroring

The unified load subsystem is intended to improve cluster stability, avoid cascading failures under pressure, and ensure that long-running jobs behave predictably in constrained environments.

---

## Transport Layer

Transport behavior has been clarified and refactored. Stream reconnection is more explicit, and all error paths now use typed errors with consistent formatting. Protocol violations, EOF, and retriable network errors are distinguished more cleanly.

Overhead has been reduced by eliminating per-connection stats and tightening stream construction. Connection-drop scenarios now propagate clearer failure signals to callers.

Specific changes include:

**Stream Recovery and Reconnection**
- Automatic stream reconnection on connection drops and resets with exponential backoff
- Stream breakage recovery (SBR) with per-sender tracking and 15-second time windows
- Reconnect signaling via `OpcReconnect` to clear SBR entries and resume operations
- Parent-child relationship between xactions/data movers and underlying streams via `TermedCB()` callback
- `ReopenPeerStream()` for replacing failed streams

**Error Handling**
- Typed transport errors (`TxErr`, `RxErr`) with consistent formatting across send and receive paths
- Clear distinction between protocol violations, benign EOF, and retriable network errors
- Fail-fast detection for known stream breakages vs timeout-based failures
- Client-side retry logic refined to only retry on write timeouts during in-flight operations

**Optimizations**
- Removed per-connection Tx/Rx stats tracking
- Streamlined stream construction (init in place)
- Tightened retry logic to reduce unnecessary connection attempts
- Connection-drop scenarios now propagate typed failure signals with full context to callers

---

## Multipart Upload

AIS v4.1 unifies multipart upload support across all major cloud backends and standardizes how AIS initiates, tracks, and completes multipart sessions. Client behavior (CLI, SDKs, aisloader) now follows a consistent interface regardless of the underlying cloud provider.

Multipart uploads are fully integrated with AIS’s chunked-object subsystem. Uploaded parts become standard AIS chunks, and the final object is represented using the same chunk-manifest format as any other chunked object.

This ensures consistent behavior across subsystems and worflows including GET, GetBatch, prefetch, rechunk, and blob-downloader.

### Unified Backend

**AWS / S3-compatible backends**
- Respect bucket-level multipart size thresholds (`extra.aws.multipart_size`)
- Full support for create → put-part → complete → abort lifecycle
- Compatibility option to disable multipart uploads for providers that reject `aws-chunked-encoding`

**Azure Backend**
- Native multipart upload via the Azure Blob Storage SDK
- Automatic staging, part management, and commit semantics
- Transparent retry and part cleanup

**GCP Backend**
- Multipart upload implemented via direct XML API requests
- Raw HTTP calls used due to lack of multipart support in the official Go SDK
- Corrected range-read metadata reporting and consistent `Last-Modified` formatting
- Reliable part tracking and final assembly

**OCI and AIS**
- Multipart flow aligned with S3 semantics
- Unified part-numbering, checksum propagation, and cleanup logic

Across all providers, multipart uploads benefit from:

- part-level retry with content-length and checksum validation
- automatic removal of partial uploads when an MPU is aborted
- unified error propagation and clearer diagnostics
- consistent final-object metadata regardless of provider

### Operational Improvements

- Multipart uploads integrate with the unified load and throttling system, backing off under memory or disk pressure.
- Large-object PUT performance improves significantly due to parallel part uploads.
- AIS now cleans up orphaned multipart parts when sessions are aborted (and is prepared for future space-cleanup extensions).
- Multipart uploads interoperate cleanly with object versioning and bucket-level provider properties.

### CLI and `aisloader`

**AIS CLI** exposes `ais object mpu` with full support for:

- `create` (session initialization)
- `put-part` (parallel uploads)
- `complete` (final assembly)
- `abort` (cleanup of uploaded parts)

**aisloader** adds:
- `-multipart-chunks` to enable multipart PUTs
- `-pctmultipart` to mix multipart and single-part uploads across workloads

These controls make it possible to benchmark multipart performance and retry behavior across cloud environments.

Multipart uploads in v4.1 provide a consistent, reliable, and cloud-agnostic way to ingest large objects, with improved failure handling and operator visibility.

---

## Python SDK

Python SDK v1.17 includes support for the updated ML endpoint, Pydantic v2, and Python 3.14. The new `Get-Batch` API replaces older multi-object loaders. Authentication test coverage has been revised, and retry behavior for 429 throttling has been improved. Additional examples and archive workflows have been added.

### Pydantic v2 Migration

Complete migration from Pydantic v1 to v2, bringing improved performance and modern validation patterns:
- `model_dump()` replaces `dict()` for serialization
- `model_validate()` and `model_validate_json()` replace parsing methods
- `RootModel` replaces `__root__` for wrapper types (e.g., `UserMap`, `RolesList`)
- Field validators use `@field_validator` and `@model_validator` decorators
- `model_config` dict replaces nested `Config` classes

This affects all model definitions including AuthN types (`UserInfo`, `RoleInfo`, `ClusterInfo`), job types (`JobSnap`, `AggregatedJobSnap`), ETL types, and internal serialization throughout the SDK.

### Get-Batch API

The initial experimental Python Get-Batch API introduced in 4.0 has been replaced by a redesigned around a new `Batch` class:

```python
# Quick batch creation
batch = client.batch(["file1.txt", "file2.txt"], bucket=bucket)

# Or build incrementally with advanced options
batch = client.batch(bucket=bucket)
batch.add("simple.txt")
batch.add("archive.tar", archpath="data/file.json")  # extract from archive
batch.add("tracked.txt", opaque=b"user-id-123")      # with tracking data

# Execute and iterate
for obj_info, data in batch.get():
    print(f"{obj_info.obj_name}: {len(data)} bytes")
```

The new API uses types (`MossIn`, `MossOut`, `MossReq`, `MossResp`) consistent with the [Go implementation](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go), supports batch reuse via automatic clearing, and provides both streaming and multipart response modes.

### Client Configuration Enhancements

The `Client` constructor now supports environment variable configuration with explicit parameter priority:

**Timeout configuration:**
- Parameter > `AIS_CONNECT_TIMEOUT` / `AIS_READ_TIMEOUT` env vars > defaults `(3, 20)`
- Setting timeout to `0` or `(0, 0)` disables all timeouts
- Individual timeouts can be disabled: `(0, 20)` disables connect timeout only

**Connection pool configuration:**
- Parameter > `AIS_MAX_CONN_POOL` env var > default `10`

This allows deployment-specific tuning without code changes while maintaining backward compatibility.

### Additional Improvements

- **AuthN manager updates**: Fixed permission value handling in `RoleManager.create()` and `update()` to use string representation, corrected type hints for optional parameters
- **Python version support**: Tested and compatible through Python 3.14
- **Type safety**: Comprehensive `Optional` type hint additions across object operations, bucket operations, and job management
- **Model path handling**: `BucketModel.get_path()` now properly formats namespace paths
- **ETL server optimization**: FastAPI server removed unnecessary `asyncio.to_thread()` wrapper, added configurable HTTP connection limits via environment variables (`MAX_CONN`, `MAX_KEEPALIVE_CONN`, `KEEPALIVE_EXPIRY`)
- **Response parsing**: URL extraction regex improved to handle edge cases in bucket and object name patterns
- **Error handling**: Better distinction between bucket-level and object-level 404 errors based on URL structure
- **Retry configuration**: Updated default retry behavior with `total=5` attempts and exponential backoff (`backoff_factor=3.0`)

---

## S3 Compatibility

**JWT Authentication via X-Amz-Security-Token:**
AIStore can now accept JWT tokens through the `X-Amz-Security-Token` header when `allow_s3_token_compat` is enabled in the configuration. This allows native AWS SDKs to authenticate using JWTs while maintaining full SigV4 compatibility.

The feature enables workload identity federation patterns where Kubernetes pods can exchange service account tokens for AIStore JWTs and authenticate S3 requests via standard AWS SDKs without requiring static credentials or long-lived tokens.

**Example AWS configuration (~/.aws/config):**
```ini
[profile aistore]
credential_process = cat ~/.config/ais/aws-credentials.json
endpoint_url = http://aistore-proxy:8080/s3
```

The credential process supplies the AIStore JWT token in the `SessionToken` field, which AWS SDKs pass through in the `X-Amz-Security-Token` header.

**Automatic JWT Fallback:**
S3 JWT authentication is automatically attempted as a fallback when SigV4 authentication is present, streamlining authentication flows for mixed client environments.

**HTTP Compliance:**
- `Last-Modified` headers now formatted in UTC to meet HTTP and S3 specification requirements
- Improved S3 API conformance for object metadata responses

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

## Tools: `aisloader`

The benchmarking and load generation tool has been updated to version 2.1 with support for archive workloads, Get-Batch operations, and efficient random-read access patterns for very large datasets.

### Get-Batch Support

With v2.1, `aisloader` can now benchmark [Get-Batch](https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md) operations using the `--get-batchsize` flag (range: 1-1000). The tool consumes TAR streams (see note below), validates archived file counts, and tracks Get-Batch-specific statistics. The `--continue-on-err` flag enables testing of soft-error handling behavior.

> Supported serialization formats include: `.tar` (default), `.tar.gz`, `.tar.lz4`, and `.zip`.

### Archive Workload Support

Archive-specific capabilities enable testing of shard-based ML workloads:

**PUT Operations:**
- Create shards at configurable percentages via `--arch.pct` (e.g., `arch.pct=30` creates 30% shards, 70% plain objects)
- Configurable archive formats: tar, tgz, zip
- Dynamic in-archive sizing with three modes: fixed count, size-bounded, or hybrid
- Optional prefix inside archives (e.g., `--arch.prefix="trunk"` or `--arch.prefix="a/b/c/trunk-"`)

**GET Operations:**
- Read archived files from existing shards
- Extract specific files via `archpath` parameter
- List-objects integration with archive-aware filtering

**Configuration:**
```bash
--arch.pct 30         # Percentage of PUTs that create shards
--arch.format tar     # Archive format (tar, tgz, zip)
--arch.num-files 100  # Files per shard (PUT only)
--arch.minsize 1KB    # Minimum file size
--arch.maxsize 10MB   # Maximum file size
--arch.prefix trunk-  # Optional prefix inside archive
```

### Random Access Across Very Large Collections

The tool uses the `name-getter` abstraction (see https://github.com/NVIDIA/aistore/blob/main/bench/tools/aisloader/namegetter/ng.go) to enable efficient random reads across very large collections: objects and archived files.

The `--epochs N` flag enables full-dataset read passes, with different algorithms selected automatically based on dataset size:

**PermAffinePrime**: For datasets larger than `100k` (by default) objects, an affine transformation with prime modulus provides memory-efficient pseudo-random access without storing full permutations. The algorithm fills batch requests completely and may span epoch boundaries.

**PermShuffle**: For datasets up to (default) `100k` objects, Fisher-Yates shuffle with uint32 indices (50% memory reduction compared to previous implementation).

**Selection Logic:**

| Workload | Dataset Size | Selected Algorithm |
|---|---:|---|
| Mixed read/write or non-epoched workloads | any | Random / RandomUnique |
| Read-only | <= `100k` objects (default) | PermShuffle |
| Read-only | >  `100k` objects (--/--) | PermAffinePrime |

> Command-line override to set the size threshold (instead of default `100k`): `--perm-shuffle-max` flag.

### Command-Line Reorganization

In v2.1, command-line parameters have been grouped into logical sections:

| Parameter Group | Purpose / Contents |
|---|---|
| `clusterParams` | Cluster connection and API configuration — proxy URL, authentication, random gateway selection |
| `bucketParams` | Target bucket and properties — bucket name, provider, JSON properties |
| `workloadParams` | Timing, intensity, and name-getter configuration — duration, workers, PUT percentage, epochs, permutation thresholds, seed, limits |
| `sizeCksumParams` | Object size constraints and integrity — min/max sizes, checksum type, hash verification |
| `archParams` | Archive/shard configuration — format, prefix, file counts, sizing |
| `namingParams` | Object naming strategy — subdirectories, file lists, virtual directories, random names |
| `readParams` | Read operation configuration — range reads, Get-Batch, latest/cached flags, eviction, error handling |
| `multipartParams` | Multipart upload settings — chunk count, percentage |
| `etlParams` | ETL configuration — predefined transforms, custom specs |
| `loaderParams` | Fleet coordination — loader ID, instance count, hash length |
| `statsParams` | Statistics and monitoring — output file, intervals, JSON format |
| `miscParams` | Cleanup, dry-run, HTTP tracing, termination control |

> For the complete list and descriptions, please see [Command-line Options](https://github.com/NVIDIA/aistore/blob/main/docs/aisloader.md#command-line-options).

### Additional Improvements

- Memory pool optimization for work orders reduces allocations
- Enhanced validation rejects invalid epoch-based runs on buckets with fewer than 2 objects
- Improved CI test coverage with Get-Batch smoke tests
- Consistent stderr usage for error logging
