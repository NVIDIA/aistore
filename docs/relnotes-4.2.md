# AIStore 4.2 Release Notes

AIStore **4.2** focuses on resilver correctness and reliability, authentication and observability, API modernization, and operational fixes across backends and tooling.

The [resilver](https://github.com/NVIDIA/aistore/blob/main/docs/resilver.md) subsystem was substantially rewritten, introducing deterministic copy selection,
explicit preemption on [mountpath](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#mountpath) events, and full support for chunked object relocation.

Authentication enhancements include Prometheus metrics for AuthN, persistent RSA key pairs, and OIDC-compatible discovery and JWKS endpoints,
improving both security posture and production visibility.

New APIs replace legacy polling with explicit condition-based waiting for xactions and introduce a chunk-aware `HEAD(object)`,
enabling more scalable job monitoring and efficient large-object access.

List-objects APIs now correctly handle non-recursive walks on AIS buckets, while the Python SDK and CLI add faster, chunk-aware download paths with parallelism and progress reporting.

Overall, this release reduces recovery risk during disk churn, improves observability and correctness under load, and modernizes long-standing APIs - while keeping full backward compatibility.

AIStore 4.2 maintains full backward compatibility with [v4.1](https://github.com/NVIDIA/aistore/releases/tag/v1.4.1) and earlier releases.

---

## Table of Contents

1. [Resilver](#resilver)
2. [Authentication and Observability](#authentication-and-observability)
3. [New APIs](#new-apis)
   - 3.1 [Xaction v2](#xaction-v2)
   - 3.2 [Object HEAD v2](#object-head-v2)
4. [List Objects: Non-Recursive Walks](#list-objects-non-recursive-walks)
5. [Multipart transfers (downloads, uploads, and backend interoperability)](#multipart-transfers)
6. [Global Rebalance](#global-rebalance)
7. [Filesystem Health Checker (FSHC)](#fshc)
8. [ETag and Last-Modified Normalization](#etag-and-last-modified-normalization)
9. [Python SDK](#python-sdk)
10. [CLI](#cli)
11. [Documentation](#documentation)
12. [Build and CI](#build-and-ci)
13. [Miscellaneous fixes across subsystems](#miscellaneous-fixes)

---

<a name="resilver"></a>
## Resilver

[Resilver](https://github.com/NVIDIA/aistore/blob/main/docs/resilver.md) is AIStore’s node-local counterpart to [global rebalance](#global-rebalance): it redistributes objects across a target’s mountpaths to restore correct placement and redundancy after volume changes (attach, detach, enable, disable).

Version 4.2 introduces a major rewrite of the resilver xaction for correctness and reliability. The previous implementation relied on retry-based copying loops that could leave objects in inconsistent states; the new implementation uses deterministic copy (object replica) selection and explicit lifecycle management.

Resilver is a _single-target_ operation: cluster-wide execution is now disallowed to prevent cross-target interference; attempts to start resilver without a target ID are rejected by both the CLI and by AIStore itself (i.e., calling the API directly without a target ID will also fail).

> [Mountpath](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#mountpath) event-triggered resilvers remain internal and continue to register with [IC](#xaction-v2) automatically (the events are: enable, disable, attach, and detach).

Improvements include:

- **Preemption on mountpath events** - disable/detach/attach/enable operations now abort any running resilver and restart appropriately; handles back-to-back mountpath events without data loss
- **Chunked object relocation** - step-wise relocation with rollback and cleanup on error; validates chunk placement
- **Mountpath jogger lifecycle** - filesystem walks terminate when the parent xaction aborts
- **Concurrent access fixes** across shared resilver state
- **Runtime progress visibility** - live counters (visited objects, active workers) via 'ais show job' CLI
- **Deterministic primary copy selection** - to eliminate contention between concurrent goroutines

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
## Authentication and Observability

Building on v4.1 authentication improvements, this release adds OIDC-compatible discovery and JWKS endpoints, persistent RSA key pairs, and production-grade observability for AuthN.

AuthN now supports both HMAC-SHA256 and RSA (RS256) token signing. When no HMAC secret is provided via config or `AIS_AUTH_SECRET_KEY`, AuthN will initialize and persist an RSA keypair on disk and use it to issue RS256-signed JWTs.

From the operational perspective, the important changes include:

* OIDC discovery + JWKS endpoints: AuthN now serves /.well-known/openid-configuration and a public JWKS endpoint for RS256 verification; JWKS responses include cache control based on token expiry.
* Cluster validation handshake updated: cluster registration validates either the HMAC secret checksum or the RSA public key, depending on the configured signing method.

Notable changes:

- **Persistent RSA key pairs for AuthN** - RSA private key is loaded from disk if present; otherwise generated once and persisted (key rotation not yet implemented)
- **OIDC discovery and JWKS endpoints** - publish configuration and public keys for standard JWT verification flows
- **Cluster validation handshake** - cluster registration validates either the HMAC secret checksum or the RSA public key, depending on signing method
- **Prometheus metrics for OIDC/JWKS**: counters for invalid iss/kid, plus latency histograms for issuer discovery and key fetches.
- **Improved logging and validation** - clearer token and ACL failure diagnostics; stricter permission checks (including admin and bucket-admin)

And separately, CLI:

- `ais authn` command, to inspect OIDC configuration and display RSA public keys
- `ais authn show oidc` and `ais authn show jwks` - to display discovery and JWKS output (JSON or table)

### Commit Highlights

* [b930247cc](https://github.com/NVIDIA/aistore/commit/b930247cc): Add metrics for total counts and JWKS caching
* [49733a2d6](https://github.com/NVIDIA/aistore/commit/49733a2d6): Refactor access check; add initial Prometheus metrics
* [4253d7bde](https://github.com/NVIDIA/aistore/commit/4253d7bde): Persistent RSA key pair for AuthN
* [2fb333675](https://github.com/NVIDIA/aistore/commit/2fb333675): Add RSA signing and validation to AuthN service
* [26e4d5098](https://github.com/NVIDIA/aistore/commit/26e4d5098): Improve logging on token/ACL failures
* [48b830470](https://github.com/NVIDIA/aistore/commit/48b830470): CLI: view OIDC config and show RSA public key
* [df8f9e764](https://github.com/NVIDIA/aistore/commit/df8f9e764): Fix CheckPermissions to validate all permission types

---

<a name="new-apis"></a>
## New APIs

<a name="xaction-v2"></a>
### 3.1 Xaction v2

Xaction (*eXtended action*) is AIStore’s abstraction for asynchronous batch jobs. All xactions expose a uniform API and CLI for starting, stopping, waiting, and reporting both generic and job-specific statistics.

Version 4.2 introduces explicit separation between IC-notifying and non-IC
[xactions](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#xaction),
replacing legacy polling with condition-based waiting and formalizing two observation modes:

- **IC-based status observation** for xactions that actively report progress via IC
- **Snapshot-based observation** for xactions that do not

For IC-notifying xactions, this avoids polling every target and makes waiting scale
predictably even in large clusters. Snapshot-based xactions continue to use explicit
snapshot inspection.

> **Background**: IC (Information Center) runs on three AIS gateways (one primary and
> two random). Targets asynchronously notify IC of xaction progress, eliminating
> per-target polling.

#### Observation APIs

| Observation type | API | Semantics |
|------------------|-----|-----------|
| **Status-based (IC)** | `GetStatus` | Return current status as reported by IC |
| | `WaitForStatus` | Block until a condition is satisfied using IC-reported status |
| **Snapshot-based** | `GetSnaps` | Fetch current xaction snapshots |
| | `WaitForSnaps` | Wait until a snapshot-based condition is satisfied |
| | `WaitForSnapsStarted` | Wait until at least one matching xaction is observed |
| | `WaitForSnapsIdle` | Wait until snapshots become empty (xaction quiescence) |

#### Built-in Conditions

Conditions are expressed as `xact.ArgsMsg` methods and apply to waiting APIs:

| Condition | Meaning |
|----------|---------|
| `Finished()` | Xaction reached a terminal state (finished or aborted) |
| `NotRunning()` | No matching xaction is currently running |
| `Started()` | At least one matching xaction has been observed |
| `Idle()` | Consecutive empty snapshots observed |

Conditions that require `OnlyRunning=true` set it automatically.

> **Note**: Python SDK support for the same v2 job-wait semantics is planned for an upcoming release.

### Commit Highlights

* [0309822081](https://github.com/NVIDIA/aistore/commit/0309822081): Introduce xaction-v2 waiting/getting APIs
* [881a6c9564](https://github.com/NVIDIA/aistore/commit/881a6c9564): Move condition implementations; define Finished semantics
* [9dec7a4c31](https://github.com/NVIDIA/aistore/commit/9dec7a4c31): Move/revise WaitForXactionIC; remove legacy polling
* [56e11b2c66](https://github.com/NVIDIA/aistore/commit/56e11b2c66): Reorg and document xaction-v2 APIs
* [3aa6b770d](https://github.com/NVIDIA/aistore/commit/3aa6b770d): CLI: multi-object jobs wait on xkind (not xname)

---

<a name="object-head-v2"></a>
### 3.2 Object HEAD v2

AIStore 4.2 introduces **Object HEAD v2**, a chunk-aware, opt-in extension of the existing HEAD(object) API that allows clients to request
structured object metadata with lower overhead and clearer access semantics.

Unlike the legacy HEAD path, Object HEAD v2 can selectively emit metadata fields based on the requested `props` set, avoiding unnecessary work and header serialization.

When requested, it exposes chunk layout information (count and maximum chunk size), enabling clients to efficiently plan parallel range reads without additional probing.

Metadata derivation was also tightened and made more consistent. `Last-Modified` and `ETag` handling was normalized across native AIS, S3 compatibility paths,
list-objects fast paths, and multipart workflows, eliminating several long-standing edge cases.

Object HEAD v2 is **fully backward-compatible**: existing clients continue to observe identical behavior unless the v2 properties are explicitly requested.

> Disclaimer: Object HEAD v2 is new and considered experimental in 4.2. While fully backward-compatible, its control structure and exposed fields
> may evolve during the 4.2 lifecycle as additional SDKs adopt it.

### Commit Highlights

* [4a1519fc6](https://github.com/NVIDIA/aistore/commit/4a1519fc6): Rewrite target ObjHeadV2; unify Last-Modified and ETag derivation
* [ce25a7ec4](https://github.com/NVIDIA/aistore/commit/ce25a7ec4): Introduce object HEAD v2 for chunk-aware access
* [b12e0b3c0](https://github.com/NVIDIA/aistore/commit/b12e0b3c0): Refactor object HEAD workflow and error handling

---

<a name="list-objects-non-recursive-walks"></a>
## List Objects: Non-Recursive Walks

Corrected non-recursive listing semantics (`ls --nr`) for AIS buckets:

- **Directory entries included** via new `IncludeDirs` walk option
- **Trailing `/` convention** enforced for directory names
- **Lexicographical ordering** for continuation tokens in non-recursive mode
- **Pagination fixes** across pages with mixed directory/file content

### Commit Highlights

* [48793d79d](https://github.com/NVIDIA/aistore/commit/48793d79d): Fix non-recursive operation on ais:// buckets
* [1a2a5eabb](https://github.com/NVIDIA/aistore/commit/1a2a5eabb): Amend CheckDirNoRecurs for non-recursive list-objects

---

<a name="multipart-transfers"></a>
## Multipart transfers (downloads, uploads, and backend interoperability)

This release significantly improves multipart data transfers across the entire stack, covering client-side downloads, server-side multipart uploads, and backend-specific interoperability.

Client-side multipart download is now available via `ais get --mpd`, enabling concurrent HTTP range reads with progress reporting.
When chunk size or object size are not explicitly provided, the implementation leverages the new `HeadObjectV2` API to discover object size and chunk layout,
ensuring efficient and predictable parallelization for large objects.

Multipart uploads now respect backend retry behavior and avoid holding excess memory or open streams across retries.
Part uploads to remote backends consistently use retriable and/or seekable readers, ensuring correctness for providers that rely on stream rewind during retries (notably Azure and remote AIS).

Under high memory pressure, AIS now applies explicit backpressure: the target waits briefly for pressure to subside and returns HTTP 429 ("Too Many Requests")
when safe buffering remains impossible, replacing the previous best-effort streaming fallback.

Backend-specific fixes further improve robustness and protocol compatibility.
S3 multipart handling now normalizes ETag quoting and derivation, disables retries for non-seekable readers to avoid SDK rewind failures, and fixes edge cases in presigned-request error handling.

Manifest validation and part lookup paths were tightened to fail fast on missing or inconsistent state, preventing silent corruption and improving error diagnostics.

Together, these changes make multipart transfers more reliable and predictable across heterogeneous backends and large deployments.

### Commit Highlights

* [aa5a32bdd](https://github.com/NVIDIA/aistore/commit/aa5a32bdd): Fix corrupted SGL offset on retry for remote AIS backend
* [21088dd26](https://github.com/NVIDIA/aistore/commit/21088dd26): S3 backend: disable retries for non-seekable readers
* [2400cf111](https://github.com/NVIDIA/aistore/commit/2400cf111): S3 backend: fix nil deref in presigned-request response path
* [3d8b2e67d](https://github.com/NVIDIA/aistore/commit/3d8b2e67d): Fix copy-bucket sync not detecting remotely-deleted objects
* [d1bdc5f52](https://github.com/NVIDIA/aistore/commit/d1bdc5f52): Fix blob download abort timeout by making Abort() non-blocking

---

<a name="global-rebalance"></a>
## Global Rebalance

Global rebalance is a special cluster-wide [xaction](#xaction-v2) that handles cluster [membership](https://github.com/NVIDIA/aistore/blob/main/docs/lifecycle_node.md) changes.
It is orchestrated by the primary (gateway), is configurable, and can also be started manually by an admin (e.g., `ais start rebalance`).

Rebalance start and completion are now serialized under a single critical section that atomically binds the renewed xaction to the singleton’s internal state (cluster map, stage, and job ID).
This prevents races during rapid, back-to-back membership changes.

Additional improvements:

- **Completion reporting** - fix cases where rebalance could be reported as finished prematurely, causing query and wait APIs to return early during membership changes
- **Atomic xaction binding** - serialize `initRenew()` and `fini()` for symmetric state transitions
- **Identity checks** - guard against unlikely race conditions
- **Persistent markers** - trigger [FSHC](#fshc) on I/O errors, surfacing repeated rebalance failures as mountpath health issues
- **Post-renew Smap handling** - validate same-targets (not same-count) and proceed safely on benign version bumps

### Commit Highlights

* [a74af7d097](https://github.com/NVIDIA/aistore/commit/a74af7d097): Fix premature rebalance completion reporting during membership changes
* [90ab46e1e](https://github.com/NVIDIA/aistore/commit/90ab46e1e): Guard atomic state; reset published pointers
* [8c97b2e6a](https://github.com/NVIDIA/aistore/commit/8c97b2e6a): Persistent markers to trigger FSHC
* [570af0e42](https://github.com/NVIDIA/aistore/commit/570af0e42): Clarify post-renew Smap change

---

<a name="fshc"></a>
## Filesystem Health Checker (FSHC)

[FSHC](https://github.com/NVIDIA/aistore/blob/main/docs/fshc.md) was improved to better distinguish fatal mountpath (disk) failures from transient I/O errors (noise), while preventing runaway re-check loops under repeated errors.

Major changes include:

- **Failure classification: FAULTED vs DEGRADED**
  - `FAULTED`: root-level failures (mount root `stat`, filesystem identity mismatch, cannot open mount root) disable the mountpath immediately.
  - `DEGRADED`: sampling read/write errors disable the mountpath only if the combined error count exceeds the configured threshold.
- **Bounded sampling**
  - FSHC runs *exactly two passes* of file sampling (read + write/fsync), stopping early when thresholds are exceeded.
- **One-shot delayed retry**
  - Root checks (and select depth-0 sampling operations) retry once with a short delay to avoid false positives from transient hiccups (common with network-attached storage).
- **Per-mountpath scheduling**
  - Checks are serialized per mountpath and rate-limited (minimum interval between runs), preventing repeated errors from triggering constant disk hammering.
- **Avoid false positives**
  - FSHC does not trigger on benign conditions such as `ENOENT` and other known non-disk failure paths.

New documentation: [`docs/fshc.md`](https://github.com/NVIDIA/aistore/blob/main/docs/fshc.md)

### Commit Highlights

* [6a1284ae1](https://github.com/NVIDIA/aistore/commit/6a1284ae1): FAULTED/DEGRADED classification; bounded sampling
* [eefa2ae6a](https://github.com/NVIDIA/aistore/commit/eefa2ae6a): Add single-delay retry; refactor unit tests

---

<a name="etag-and-last-modified-normalization"></a>
## ETag and Last-Modified Normalization

Strict consistency for metadata handling across the codebase:

- **Unquoted internally, quoted on the wire** - applies to headers and S3 XML responses
- **`lom.ETag()`** - prefer custom cloud ETag; fall back to MD5 when available; otherwise generate a stable synthetic value
- **`lom.LastModified()` / `lom.LastModifiedLso()`** - avoid mtime syscalls in list-objects fast paths
- **S3 list-objects** - amend wanted metadata on a best-effort basis without extra syscalls

These changes unify metadata semantics across native AIS, S3 compatibility paths, and list-objects fast paths.

### Commit Highlights

* [33c987cd4](https://github.com/NVIDIA/aistore/commit/33c987cd4): Normalize ETag and Last-Modified; generate both if needed
* [8fb294819](https://github.com/NVIDIA/aistore/commit/8fb294819): Quote ETag in XML responses (ListParts, MPU complete)
* [4d6873bb6](https://github.com/NVIDIA/aistore/commit/4d6873bb6): ETag quoting/unquoting rules: strict consistency

---

<a name="python-sdk"></a>
## Python SDK

The Python SDK gained **chunk-aware range reading** built on the new, **experimental `HeadObjectV2` API**.
The API exposes object chunk layout (`ObjectPropsV2.Chunks`), allowing the SDK to issue **chunk-aligned range requests** and reassemble chunks in sequential order.
This enables optional **parallel reads for large objects** via `Object.get_reader(num_workers=N)`, particularly beneficial for erasure-coded and chunked objects.

> `HeadObjectV2` is introduced as an **experimental interface in 4.2**; while the semantics are stable, the underlying control structures may evolve during the 4.2 iteration.

HTTPS and cluster-key authentication reliability was improved by fixing HMAC signature mismatches on redirects (HTTP 401) through correct `Content-Length` handling for byte buffers, strings, and file-like objects,
and by refining redirect logic to avoid SSL EOF errors when request bodies are present.

Job-waiting behavior was unified across long-running operations. `Job.wait()`, `wait_for_idle()`, `wait_single_node()`, and `Dsort.wait()` now return a structured `WaitResult` (success flag, error, and completion time),
improving timeout handling and diagnostics for aborted or preempted jobs.

Finally, Python access-control constants were synchronized with AIS semantics.
This includes updated read/write scopes, corrected cluster-level access flags, and alignment with bucket-admin permissions to match the server-side authorization model.

### Commit Highlights

* [036361cff](https://github.com/NVIDIA/aistore/commit/036361cff): Add chunk-aware object reader with concurrent range-reads
* [744f5a0b6](https://github.com/NVIDIA/aistore/commit/744f5a0b6): Add multipart download API with concurrent range-reads
* [51047e075](https://github.com/NVIDIA/aistore/commit/51047e075): Add progress report callback to MultipartDownload
* [34d772ea4](https://github.com/NVIDIA/aistore/commit/34d772ea4): Fix HMAC mismatch and improve request handling
* [f3a6504cb](https://github.com/NVIDIA/aistore/commit/f3a6504cb): Implement WaitResult dataclass for consistent job wait API
* [94f1e0ec1](https://github.com/NVIDIA/aistore/commit/94f1e0ec1): Release Python SDK version 1.19.0
* [7662730c3](https://github.com/NVIDIA/aistore/commit/7662730c3): Add `requests_list` property to retrieve MossIn requests in Batch class

---

<a name="cli"></a>
## CLI

The CLI now provides safer handling of object names containing special symbols across more commands.
The existing `--encode-objname` option is recognized in additional code paths (including GET/PUT, rename, multipart, ETL object, and others).
When the flag isn’t provided and the name appears to require escaping, the CLI emits a one-time warning suggesting `--encode-objname`.

> Note: do not combine `--encode-objname` with already-escaped names to avoid double-encoding.

For large downloads, `ais get` adds `--mpd` for client-side multipart (HTTP range) download of a single object, with a progress bar.
Related flags (`--chunk-size`, `--num-workers`) are now validated and require either `--mpd` or `--blob-download`.

> Note: the `ais get` command-line options `--mpd` and `--blob-download` are mutually exclusive.

Authentication tooling is expanded with `ais auth show oidc` and `ais auth show jwks`, providing direct visibility into AuthN OIDC configuration and public JWKS.

Resilvering commands were tightened to require an explicit target ID and now detect and warn about an already running resilver before starting a new one.
Several usage and help texts were improved, and `ais ls --no-recursion` now lists directories before objects.

In short, the release includes:

- **Multipart download** - `ais get --mpd` with configurable `--chunk-size` and `--num-workers`, including progress bar support
- **Resilver shortcut** - `ais storage resilver` alias; confirmation when a resilver is already running
- **Special character encoding** - `--encode-objname` across put/get/remove/rename; centralized warning
- **Extended help** - `ais create --help` now enumerates six common usage patterns with examples
- **Mountpath commands** - expanded usage text for attach/detach/enable/disable

### Commit Highlights

* [876264b9c](https://github.com/NVIDIA/aistore/commit/876264b9c): Add client-side multipart download to `ais get`
* [08eb7224a](https://github.com/NVIDIA/aistore/commit/08eb7224a): Add `ais storage resilver` alias; confirm when already running
* [48d247d7c](https://github.com/NVIDIA/aistore/commit/48d247d7c): Warn/encode special symbols in object names across operations
* [bb44f2179](https://github.com/NVIDIA/aistore/commit/bb44f2179): Add extended `ais create --help`
* [e2837ed8b](https://github.com/NVIDIA/aistore/commit/e2837ed8b): Add extended usage for mountpath commands
* [bde86af9c](https://github.com/NVIDIA/aistore/commit/bde86af9c): Support remote bucket props (`profile`, `endpoint`) at creation time with `--skip-lookup`
* [d413e3a2c](https://github.com/NVIDIA/aistore/commit/d413e3a2c): Refactor bucket creation; revise `--skip-lookup` path

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

CI pipelines now run larger clusters with remote AIS enabled, improving test coverage for distributed scenarios.
Infrastructure and dependency updates include:

* [50f00f8c4](https://github.com/NVIDIA/aistore/commit/50f00f8c4): GitHub CI: run larger cluster with remote AIS
* [ece283da6](https://github.com/NVIDIA/aistore/commit/ece283da6): GitLab CI: enable remote AIS cluster (`remais`) in long tests
* [9be9f67d1](https://github.com/NVIDIA/aistore/commit/9be9f67d1): Unify host:port and URL construction; remove IPv4-only assumptions
* [97da81299](https://github.com/NVIDIA/aistore/commit/97da81299): Parallelize Docker image builds
* [d2aa6a1fc](https://github.com/NVIDIA/aistore/commit/d2aa6a1fc): Add pipeline to randomize bucket namespacing
* [0af6a913d](https://github.com/NVIDIA/aistore/commit/0af6a913d): Upgrade OSS dependencies

<a name="miscellaneous-fixes"></a>
## Miscellaneous fixes across subsystems

- **Concurrency and ordering**
  - [`b5590a094`](https://github.com/NVIDIA/aistore/commit/b5590a094) Fix `OpcDone` ordering race - ensure completion sentinel (indicating end of Tx) cannot arrive before Rx data
  - [`d1bdc5f52`](https://github.com/NVIDIA/aistore/commit/d1bdc5f52) Non-blocking abort for blob-download xactions - prevent premature abort completion under concurrent shutdown
  - [`aa5a32bdd`](https://github.com/NVIDIA/aistore/commit/aa5a32bdd) Multipart retry correctness - fix corrupted SGL offsets during retries against remote AIS backends

- **List-objects and metadata**
  - [`48793d79d`](https://github.com/NVIDIA/aistore/commit/48793d79d) Non-recursive listing semantics - correct `list-objects` operation for `ais://` buckets
  - [`1a2a5eabb`](https://github.com/NVIDIA/aistore/commit/1a2a5eabb) Directory handling fix - amend `CheckDirNoRecurs` logic for edge cases
  - [`765953ebd`](https://github.com/NVIDIA/aistore/commit/765953ebd) Cloud namespace metadata - fix BMD handling for cloud buckets with namespaces
  - [`45272ceef`](https://github.com/NVIDIA/aistore/commit/45272ceef): Enforce 2KiB size limit for custom object metadata

- **Auth and API correctness**
  - [`df8f9e764`](https://github.com/NVIDIA/aistore/commit/df8f9e764) Permission validation - ensure `CheckPermissions` validates all permission types
  - [`22b599760`](https://github.com/NVIDIA/aistore/commit/22b599760) Early request rejection - return early on invalid object rename requests

- **Backend and sync behavior**
  - [`3d8b2e67d`](https://github.com/NVIDIA/aistore/commit/3d8b2e67d) Copy-bucket sync correctness - detect objects deleted remotely from the destination
  - [`2400cf111`](https://github.com/NVIDIA/aistore/commit/2400cf111) S3 presigned requests - fix nil dereference in error-handling path
