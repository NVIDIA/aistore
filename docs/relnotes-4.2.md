# AIStore 4.2 Release Notes

## Overview

AIStore **4.2** focuses on resilver reliability, authentication/authorization, observability, and API modernization.

AIS [resilver](https://github.com/NVIDIA/aistore/blob/main/docs/resilver.md) is substantially rewritten with deterministic copy selection, proper preemption on mountpath events, and full support for chunked object relocation.

Authentication gains Prometheus metrics and persistent RSA key pairs for AuthN.

New Go APIs replace legacy polling with explicit condition-based waiting for xactions and introduce chunk-aware HEAD(object). List-objects (API) now correctly handles non-recursive walks on AIS buckets.

The Python SDK adds concurrent range-read downloads for chunked objects, and the CLI gains multipart download support with progress reporting.

This release significantly reduces recovery risk during disk churn, improves visibility into authentication failures in production, and modernizes long-standing APIs without breaking existing clients.

This release maintains full backward compatibility with [v4.1](https://github.com/NVIDIA/aistore/releases/tag/v1.4.1) and prior releases.

---

**Table of Contents**

1. [Resilver](#resilver)
2. [Authentication and Observability](#authentication-and-observability)
3. [New Go APIs](#new-go-apis)
   - 3.1 [Xaction v2](#xaction-v2)
   - 3.2 [Object HEAD v2](#object-head-v2)
4. [List Objects: Non-Recursive Walks](#list-objects-non-recursive-walks)
5. [Multipart and Backend Fixes](#multipart-and-backend-fixes)
6. [Global Rebalance](#global-rebalance)
7. [FSHC Improvements](#fshc-improvements)
8. [ETag and Last-Modified Normalization](#etag-and-last-modified-normalization)
9. [Python SDK](#python-sdk)
10. [CLI Enhancements](#cli-enhancements)
11. [Documentation](#documentation)
12. [Build and CI](#build-and-ci)

---

<a name="resilver"></a>
## Resilver

Major rewrite of the resilver xaction for correctness and reliability. The previous implementation relied on retry-based copying loops that could leave objects in inconsistent states; v4.2 introduces deterministic copy selection and explicit lifecycle management.

Key changes:

- **Deterministic primary copy selection**
- **Preemption on mountpath events** - disable/detach/attach/enable operations now abort any running resilver xaction and restart appropriately; handles back-to-back mountpath events without data loss
- **Chunked object relocation** - with step-wise implementation and rollback/cleanup on any error; validates chunk placement
- **Mountpath Jogger lifecycle improvements** - filesystem walks terminate when parent xaction aborts
- **Concurrent access fixes**

As of 4.2, AIS resilver ensures that each stored object (including chunked objects) is rebuilt from a single, well-defined source copy or fails atomically, with no partial visibility.

New documentation: [`docs/resilver.md`](https://github.com/NVIDIA/aistore/blob/main/docs/resilver.md)

### Commit Highlights

* [805f9ab93](https://github.com/NVIDIA/aistore/commit/805f9ab93): Rewrite copy-recovering path; deterministic primary copy selection
* [48153ca6d](https://github.com/NVIDIA/aistore/commit/48153ca6d): Preempt running xaction upon mountpath events; add tests
* [55c890fdd](https://github.com/NVIDIA/aistore/commit/55c890fdd): Relocate chunked objects with rollback
* [140c6e432](https://github.com/NVIDIA/aistore/commit/140c6e432): Stop joggers; revise concurrent access to shared state
* [d2a56a220](https://github.com/NVIDIA/aistore/commit/d2a56a220): Add stress tests; consolidate resilver tests
* [1a53f10e9](https://github.com/NVIDIA/aistore/commit/1a53f10e9): Revise mountpath jogger; add walk-stopped sentinel
* [96213840f](https://github.com/NVIDIA/aistore/commit/96213840f): Wire walks to parent xaction abort

---

<a name="authentication-and-observability"></a>

<a name="authentication-and-observability"></a>
## Authentication and Observability

Building on v4.1's authentication improvements, this release adds OIDC-compatible discovery + JWKS endpoints

> AuthN serves /.well-known/openid-configuration and publishes its public JWKS for RSA verification

- **Token signing methods: HMAC-SHA256 and RSA (RS256)** - AuthN can issue RSA-signed JWTs in addition to HMAC
- **Persistent RSA key pairs for AuthN** - Persistent RSA key pairs for AuthN - RSA private key is loaded from disk if present; otherwise generated once and saved (key rotation not yet implemented)
- **OIDC discovery + JWKS endpoints** - AuthN can serve `/.well-known/openid-configuration` and publish its public JWKS for RSA verification
- **Cluster validation handshake** - cluster registration now validates either the HMAC secret checksum or the RSA public key, depending on the configured signing method
- **Prometheus metrics and latency histograms** - counters for invalid issuer/kid, and latency tracking for OIDC discovery and JWKS/key fetches
- **Improved logging** on token and ACL failures, and stricter permission validation (including admin/bucket-admin permission types)
- **CLI enhancements** - view OIDC config and display RSA public keys via `ais authn`

### Commit Highlights

* [b930247cc](https://github.com/NVIDIA/aistore/commit/b930247cc): Add metrics for total counts and JWKS caching
* [49733a2d6](https://github.com/NVIDIA/aistore/commit/49733a2d6): Refactor access check; add initial Prometheus metrics
* [4253d7bde](https://github.com/NVIDIA/aistore/commit/4253d7bde): Persistent RSA key pair for AuthN
* [2fb333675](https://github.com/NVIDIA/aistore/commit/2fb333675): Add RSA signing and validation to AuthN service
* [26e4d5098](https://github.com/NVIDIA/aistore/commit/26e4d5098): Improve logging on token/ACL failures
* [48b830470](https://github.com/NVIDIA/aistore/commit/48b830470): CLI: view OIDC config and show RSA public key
* [df8f9e764](https://github.com/NVIDIA/aistore/commit/df8f9e764): Fix CheckPermissions to validate all permission types


---

<a name="new-go-apis"></a>
## New Go APIs

<a name="xaction-v2"></a>
### 3.1 Xaction v2

New Go API with explicit separation between IC-notifying and non-IC xactions. This replaces the legacy `_waitx()` polling machinery with condition-based waiting.

**Background**: IC (Information Center) is implemented by 3 AIS gateways (one primary + 2 random). Targets asynchronously notify IC of xaction progress via `nl.Status`, eliminating the need to poll each target. Not all xactions emit IC notifications, which necessitates snapshot-based waiting.

**Status-based** (IC-notifying xactions):
- `WaitForStatus`, `GetStatus`

**Snaps-based** (non-IC xactions):
- `WaitForSnaps`, `GetSnaps`, `WaitForSnapsStarted`, `WaitForSnapsIdle`

**Built-in conditions** as `xact.ArgsMsg` methods:
- `Finished()` - terminal state (finished/aborted)
- `NotRunning()` - not currently running
- `Started()` - at least one matching xaction observed
- `Idle()` - consecutive empty snapshots

Conditions that require `OnlyRunning=true` set it automatically.

The legacy polling helpers are now considered deprecated and will be removed in a future release.

> **Note**: Python SDK updates supporting the same v2 job-wait semantics are planned for an upcoming release.

### Commit Highlights

* [0309822081](https://github.com/NVIDIA/aistore/commit/0309822081): Introduce xaction-v2 waiting/getting APIs
* [881a6c9564](https://github.com/NVIDIA/aistore/commit/881a6c9564): Move condition implementations; define Finished semantics
* [9dec7a4c31](https://github.com/NVIDIA/aistore/commit/9dec7a4c31): Move/revise WaitForXactionIC; remove legacy polling
* [56e11b2c66](https://github.com/NVIDIA/aistore/commit/56e11b2c66): Reorg and document xaction-v2 APIs
* [3aa6b770d](https://github.com/NVIDIA/aistore/commit/3aa6b770d): CLI: multi-object jobs wait on xkind (not xname)

<a name="object-head-v2"></a>
### 3.2 Object HEAD v2

Chunk-aware object HEAD with selective header emission for reduced overhead:

- Parse `props` parameter to emit only requested headers
- Factor in chunked header logic
- Unified Last-Modified/ETag derivation from a single mtime source
- Micro-optimizations for header serialization

Object HEAD v2 is behavior-preserving for existing clients and only affects internal execution paths unless explicitly requested via `props`.

### Commit Highlights

* [4a1519fc6](https://github.com/NVIDIA/aistore/commit/4a1519fc6): Rewrite target's ObjHeadV2; unify Last-Modified/ETag
* [ce25a7ec4](https://github.com/NVIDIA/aistore/commit/ce25a7ec4): Introduce object HEAD v2 for chunk-aware GET
* [b12e0b3c0](https://github.com/NVIDIA/aistore/commit/b12e0b3c0): Refactor object HEAD workflow and error handling

---

<a name="list-objects-non-recursive-walks"></a>
## List Objects: Non-Recursive Walks

Corrected non-recursive listing (`ls --nr`) semantics for AIS buckets:

- **Directory entries included** with new `IncludeDirs` walk option
- **Trailing `/` convention** enforced for directory names
- **Lexicographical ordering** for continuation tokens in non-recursive mode
- **Pagination fixes** across pages with mixed dirs/files content

### Commit Highlights

* [48793d79d](https://github.com/NVIDIA/aistore/commit/48793d79d): Fix non-recursive operation on ais:// buckets
* [1a2a5eabb](https://github.com/NVIDIA/aistore/commit/1a2a5eabb): Amend CheckDirNoRecurs for non-recursive list-objects

---

<a name="multipart-and-backend-fixes"></a>
## Multipart and Backend Fixes

**Retry correctness**
- **SGL offset corruption fix** - wrap SGL in `memsys.Reader` for backends requiring seekable/retriable readers (Azure, remote AIS)

**Memory pressure handling**
- Wait and retry under memory pressure; reject with HTTP 429 if allocation fails

**Backend-specific fixes**
- **S3 backend** - disable retries for non-seekable readers; fix nil deref in presigned-request response path

**Long-running job cleanup**
- **Copy-bucket sync** - fix detection of remotely-deleted objects from destination
- **Blob downloader abort** - make `Abort()` non-blocking; cleanup via `finalize()` in `Run()` path

### Commit Highlights

* [aa5a32bdd](https://github.com/NVIDIA/aistore/commit/aa5a32bdd): Fix corrupted SGL offset on retry for remote AIS backend
* [21088dd26](https://github.com/NVIDIA/aistore/commit/21088dd26): S3 backend: disable retries for non-seekable readers
* [2400cf111](https://github.com/NVIDIA/aistore/commit/2400cf111): S3 backend: fix nil deref in presigned-request response
* [3d8b2e67d](https://github.com/NVIDIA/aistore/commit/3d8b2e67d): Fix copy-bucket sync not detecting remotely-deleted objects
* [d1bdc5f52](https://github.com/NVIDIA/aistore/commit/d1bdc5f52): Fix blob download abort timeout by making Abort() non-blocking

---

<a name="global-rebalance"></a>
## Global Rebalance

Improved state management and error handling:

- **Atomic xaction binding** - serialize `initRenew()` and `fini()` for symmetric state transitions
- **Identity check in fini()** - guard against unlikely race conditions
- **Persistent markers** trigger FSHC on I/O errors, ensuring repeated rebalance failures surface as mountpath health issues
- **Clarified post-renew Smap change** - same-targets validation (not same-count)

### Commit Highlights

* [90ab46e1e](https://github.com/NVIDIA/aistore/commit/90ab46e1e): Guard atomic state; reset published pointers
* [8c97b2e6a](https://github.com/NVIDIA/aistore/commit/8c97b2e6a): Persistent markers to trigger FSHC
* [570af0e42](https://github.com/NVIDIA/aistore/commit/570af0e42): Clarify post-renew Smap change

---

<a name="fshc-improvements"></a>
## FSHC Improvements

Filesystem Health Checker gains failure classification and bounded sampling:

- **FAULTED vs DEGRADED** - root stat/fsid/open failures classified as FAULTED (immediate disable); I/O errors during sampling classified as DEGRADED
- **Bounded sampling loop** - prevents unbounded retries on dying disks
- **Single-delay retry** for root-level failures (accommodates network storage transients such as NFS/NVMesh)
- **Avoid false positives** - skip FSHC trigger on ENOENT markers and empty directories

For example, transient network storage hiccups no longer immediately disable mountpaths.

New documentation: [`docs/fshc.md`](https://github.com/NVIDIA/aistore/blob/main/docs/fshc.md)

### Commit Highlights

* [6a1284ae1](https://github.com/NVIDIA/aistore/commit/6a1284ae1): FAULTED/DEGRADED classification; bounded sampling
* [eefa2ae6a](https://github.com/NVIDIA/aistore/commit/eefa2ae6a): Add single-delay retry; refactor unit tests

---

<a name="etag-and-last-modified-normalization"></a>
## ETag and Last-Modified Normalization

Strict consistency for metadata handling across the codebase:

- **Unquoted internally, quoted on the wire** - response headers and S3 XML responses
- **lom.ETag()** - uses custom (cloud) ETag if available, MD5 when available, otherwise generates a stable synthetic value
- **lom.LastModified() / lom.LastModifiedLso()** - avoid mtime syscalls in list-objects fast paths
- **S3 list-objects** - amend wanted metadata (best-effort, no syscalls)

These changes unify metadata semantics across native AIS, S3 compatibility paths, and list-objects fast paths.

### Commit Highlights

* [33c987cd4](https://github.com/NVIDIA/aistore/commit/33c987cd4): Normalize ETag and Last-Modified; generate both if needed
* [8fb294819](https://github.com/NVIDIA/aistore/commit/8fb294819): Quote ETag in XML responses (ListParts, MPU complete)
* [4d6873bb6](https://github.com/NVIDIA/aistore/commit/4d6873bb6): ETag quoting/unquoting rules: strict consistency

---

<a name="python-sdk"></a>
## Python SDK

- **Concurrent range-read downloads** - `ParallelContentIterProvider` fetches chunks using concurrent HTTP range-reads while yielding in sequential order
- **num_workers parameter** in `Object.get_reader()` triggers parallel download
- **Multipart download API** with progress report callbacks
- **HMAC and content-length fixes** - calculate content length for all data types; fix HTTPS manual redirect SSL EOF errors
- **WaitResult dataclass** for consistent job-wait API

> **Note**: Python SDK will be soon updated to support the same v2 job-wait semantics.

### Commit Highlights

* [036361cff](https://github.com/NVIDIA/aistore/commit/036361cff): Add chunk-aware object reader with concurrent range-reads
* [744f5a0b6](https://github.com/NVIDIA/aistore/commit/744f5a0b6): Add multipart download API with concurrent range-reads
* [51047e075](https://github.com/NVIDIA/aistore/commit/51047e075): Add progress report callback to MultipartDownload
* [34d772ea4](https://github.com/NVIDIA/aistore/commit/34d772ea4): Fix HMAC mismatch and improve request handling
* [f3a6504cb](https://github.com/NVIDIA/aistore/commit/f3a6504cb): Implement WaitResult dataclass for consistent job wait API

---

<a name="cli-enhancements"></a>
## CLI Enhancements

- **Multipart download** - `ais get --mpd` with configurable `--chunk-size` and `--num-workers`; progress bar support
- **Resilver shortcut** - `ais storage resilver` (alias for `ais job start resilver`); confirms when already running
- **Special character encoding** - `--encode` option across put/get/remove/rename operations; centralized `warnEscapeObjName()`
- **Extended help** - `ais create --help` enumerates six usage patterns with examples
- **Mountpath commands** - extended usage for attach/detach/enable/disable

These changes reduce friction for large object workflows and improve safety around object naming across platforms.

### Commit Highlights

* [876264b9c](https://github.com/NVIDIA/aistore/commit/876264b9c): Add client-side multipart download to `ais get`
* [08eb7224a](https://github.com/NVIDIA/aistore/commit/08eb7224a): Add `ais storage resilver` alias; confirm when running
* [48d247d7c](https://github.com/NVIDIA/aistore/commit/48d247d7c): Warn/encode special symbols in objname across operations
* [bb44f2179](https://github.com/NVIDIA/aistore/commit/bb44f2179): Add extended `ais create --help`
* [e2837ed8b](https://github.com/NVIDIA/aistore/commit/e2837ed8b): Add extended usage for mountpath commands

---

<a name="documentation"></a>
## Documentation

- **New**: [`docs/resilver.md`](https://github.com/NVIDIA/aistore/blob/main/docs/resilver.md) - resilver architecture and operations
- **New**: [`docs/fshc.md`](https://github.com/NVIDIA/aistore/blob/main/docs/fshc.md) - filesystem health checker
- **Rewritten**: [`docs/bucket.md`](https://github.com/NVIDIA/aistore/blob/main/docs/bucket.md) - *AIS Buckets: Design and Operations*
- **Revised**: [`docs/http_api.md`](https://github.com/NVIDIA/aistore/blob/main/docs/http_api.md) - updated cross-references
- **Python SDK docs** - environment variables and configuration precedence

### Commit Highlights

* [63fb898e3](https://github.com/NVIDIA/aistore/commit/63fb898e3): Add docs/resilver.md
* [7a57f2913](https://github.com/NVIDIA/aistore/commit/7a57f2913): New docs/bucket.md - design and operations
* [759c7fc61](https://github.com/NVIDIA/aistore/commit/759c7fc61): Clarify bucket creation vs identity
* [291f428a0](https://github.com/NVIDIA/aistore/commit/291f428a0): Python SDK: add env vars and config precedence

---

<a name="build-and-ci"></a>
## Build and CI

- **IPv6-safe parsing and host:port string conversions** - unify host:port and URL construction; remove IPv4-only assumptions
- **Darwin builds** - minor refactor and test updates
- **GitLab CI** - enable remais in long tests; add pipeline to randomize bucket namespacing
- **Dependency bumps**:
  - actions/checkout v5 => v6
  - actions/upload-artifact v5 => v6
  - actions/download-artifact v6 => v7
  - golangci/golangci-lint-action v8 => v9
  - GCS SDK 1.57 => 1.58
  - AWS SDK 1.39 => 1.40
  - fasthttp 1.67 => 1.68
- **Parallel Docker builds** in CI workflow

### Commit Highlights

* [9be9f67d1](https://github.com/NVIDIA/aistore/commit/9be9f67d1): Unify host:port and URL construction; remove IPv4-only assumptions
* [ece283da6](https://github.com/NVIDIA/aistore/commit/ece283da6): GitLab CI: enable remais in long tests
* [d2aa6a1fc](https://github.com/NVIDIA/aistore/commit/d2aa6a1fc): Add pipeline to randomize bucket namespacing
* [97da81299](https://github.com/NVIDIA/aistore/commit/97da81299): Parallelize Docker image builds
* [0af6a913d](https://github.com/NVIDIA/aistore/commit/0af6a913d): Upgrade all OSS packages
