# Changelog

All notable changes to the AIStore Python SDK project are documented in this file.

We structure this changelog in accordance with [Keep a Changelog](https://keepachangelog.com/) guidelines, and this project follows [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `start_after` parameter on `Bucket.list_objects()`, `list_objects_iter()`,
  `list_all_objects()`, and `list_all_objects_iter()` (and `ListObjectsMsg`),
  mirroring Go's `LsoMsg.StartAfter`: lists objects whose names are strictly
  greater than the given marker — useful for sharding flat-bucket enumeration
  across parallel listers. Applied to the first page only (subsequent pages
  resume via the continuation token). AIS buckets only; raises
  `NotImplementedError` for cloud buckets.
- Get-Batch byte-range reads: `Batch.add()` now honors the `start`/`length`
  parameters to retrieve a byte range of an object (chunked or monolithic) or of
  a file extracted from an archive (when `archpath` is set).
- `aistore.sdk.xact_const` mirroring Go's `xact/api_table.go`: `XACT_KIND_*`
  string constants, `IDLE_KINDS` / `KNOWN_KINDS` frozensets, and the
  `idles_before_finishing()` / `is_valid_kind()` predicates.
- `Job.abort()` mirroring Go's `api.AbortXaction`: stops a job scoped by its
  `id` and/or `kind`. After aborting, `wait()` returns a `WaitResult` with
  `success=False` and the abort error instead of blocking until timeout.

### Changed

- `Job.wait()` is now descriptor-aware, mirroring Go's
  `api.WaitForXaction`: when `job_kind` is an idle kind (e.g. `download`,
  `get-batch`, `copy-listrange`, `etl-listrange`, `archive`, `list`,
  `put-copies`, `ec-get`/`ec-put`/`ec-resp`) it waits for cluster-wide idle;
  otherwise — including
  empty or unknown kinds — it waits for terminal state (preserving the
  pre-convergence behavior). `wait_for_idle` and `wait_single_node` remain
  available as explicit overrides.
- `Job.wait()` resolves the job kind from the cluster when only a job id is
  given (raising `JobInfoNotFound` if the id is unknown), so a job created
  with just an id still dispatches correctly.
- `Job.wait()` / `wait_for_idle()` require an idle-kind job to report idle on
  2 consecutive polls before completing (mirrors Go's
  `xact.numConsecutiveIdle`); an abort on any target still returns immediately.

### Fixed

- ETL webservers now forward
  `etl_args` to the next stage on direct-put pipeline hops. Previously only the
  first pipeline stage received `etl_args`; stages 2..N saw an empty value.

## [1.25.0] - 2026-05-20

### Added

- `AIS_SKIP_VERIFY_CRT` environment variable to disable SSL certificate
  verification (matches the Go CLI / `aisloader` naming). The legacy
  `AIS_SKIP_VERIFY` continues to work for backward compatibility. Truthy values
  are parsed via a new `parse_bool` helper in `aistore.sdk.utils` that mirrors
  Go's `cos.ParseBool` (accepts `1/t/true/y/yes/on`, case-insensitive).

### Changed

- Updated cold-GET retry delay mechanism to check write-lock status rather than object presence.
  - Added `enable_remote_head` option to `ColdGetConf` determine if size estimation should query remote backend when missing local object metadata.
- Raised default cold-GET estimated bandwidth for retry delay from 1Gbps to 10Gbps.

### Fixed

- Requests for cold-GET retry polling will now go to the SDK's configured AIS endpoint instead of modifying the target request.
  - Preserves compatibility with cluster-key redirect signing.
- ETL `FlaskServer`: stream no-FQN `hpush` PUTs in constant memory via Werkzeug
  `request.stream` (with chunked drain-on-close to keep keep-alive sockets
  clean); release upstream `hpull` GET connections through `_ResponseRawReader`;
  forward upstream HTTP error statuses as-is instead of collapsing them to 502.
- ETL `FlaskServer` joins the **ETL → AIS retry contract** (streaming no-FQN
  PUT): one-shot bail emits HTTP 503 with
  `Ais-Etl-Retry-Reason: direct-put-transient` so AIS retries against the
  replayable LOM-backed source; replayable-exhausted cases keep emitting 502.
  Matches the behavior already in `FastAPIServer` and `HTTPMultiThreadedServer`.

## [1.24.0] - 2026-05-08

### Changed

- **Retry logging**: emit the failing request's call stack at ERROR
  exactly once when retries are exhausted (tenacity
  `retry_error_callback`, replacing bare `reraise=True`); the per-attempt
  `before_sleep_log` WARNING now actually fires under the default config
  (`RetryManager` previously overwrote the configured `before_sleep` with
  its cold-get hook).

### Fixed

- **PyTorch DataLoader picklability** (Python 3.14+ POSIX, macOS, Windows,
  any explicit `spawn`/`forkserver`): made `RetryConfig`/`RetryManager`
  survive `pickle` so `AISIterDataset`/`AISMapDataset` can be sent to
  `DataLoader(num_workers > 0)` workers. The non-picklable
  `tenacity.Retrying` (lambda predicate, `before_sleep_log` closure) is
  dropped on serialize and rebuilt from `RetryConfig.default()` in the
  worker — caller-customized tenacity policy is replaced by the default
  in workers; other fields (`http_retry`, `cold_get_conf`) survive
  unchanged. Single-process usage is unaffected.
- **`ObjectFileReader` clean short-EOF recovery**: detect streams that end
  without raising but deliver fewer bytes than the GET `Content-Length`
  advertised, and resume from the last delivered byte instead of returning a
  truncated read. Recovery shares the existing broken-stream path (cached-
  presence check, `max_resume` bound, restart-from-zero on eviction).
  Detection is skipped when the response carries a non-identity
  `Content-Encoding`.
- ETL `FastAPIServer`: stream no-FQN `hpull` GETs in constant memory via the
  shared `requests.Session`, wrapped in `asyncio.to_thread` to keep the event
  loop responsive. Previously the full upstream object was buffered into memory
  before being handed to `transform_stream`.
- ETL `HTTPMultiThreadedServer`: release upstream keep-alive connections
  through response-level close on early-close, error, and client-disconnect
  paths (previously `resp.raw.close()` skipped `release_conn()` and leaked
  sockets from the urllib3 pool). Forward upstream HTTP error statuses as-is
  instead of collapsing them, and map `requests.RequestException` to HTTP 502
  (was 500) for the no-FQN `hpull` GET path.
- ETL `FastAPIServer`: stream no-FQN `hpush` PUTs in constant memory,
  mirroring the `hpull` GET change. Transient direct-put errors surface to AIS
  to retry the whole PUT (request body is one-shot).
- ETL `HTTPMultiThreadedServer`: stream no-FQN `hpush` PUTs in constant
  memory. Previously the full request body was read into a `BytesIO`
  before being handed to `transform_stream`.
- **ETL → AIS retry contract** (streaming no-FQN PUT): when the ETL bails on a
  transient direct-put failure *without* trying locally (one-shot body case),
  `FastAPIServer` and `HTTPMultiThreadedServer` now respond with HTTP 503 and
  the `Ais-Etl-Retry-Reason: direct-put-transient` header. AIS treats that pair
  as a soft error and retries the whole PUT against the replayable LOM-backed
  source. Other paths (buffered/FQN/GET, `FlaskServer`) keep emitting 502 on
  `direct_put_retries` exhaustion — they already retried locally, so AIS
  retrying on top would just be amplification.
- **ETL direct-put retry**: added exponential-backoff retry for transient connection
  errors in Flask and HTTP multi-threaded ETL servers for parity with FastAPI.
  `ConnectionRefused` is now treated as a permanent error that returns HTTP 502
  across all server types.

### Removed

- Removed legacy `aistore/client.py` compat re-export (torchdata shim for SDK > 1.04).

## [1.23.0] - 2026-03-25

### Added

- **MCP server** (experimental): initial `aistore.mcp` integration for AI agents
  (Claude, Cursor, Windsurf, etc.). Read-only tools for cluster health/info/performance,
  bucket listing/summary, object listing/metadata, job status, and ETL details/logs.
  Install via `pip install aistore[mcp]`.
- **Native Bucket Inventory (NBI)**: `Bucket.create_inventory()`,
  `Bucket.destroy_inventory()`, `Bucket.show_inventory()` for managing bucket inventories.
  `inventory_name` parameter added to `list_objects()`, `list_objects_iter()`, and
  `list_all_objects()` to list objects from an NBI snapshot.
- **Parallel download `read_all()`**: `ObjectReader.read_all()` with `num_workers`
  now returns a `ParallelBuffer` backed by `SharedMemory` — eliminates the extra
  memory copy previously required when joining chunks from the ring buffer.
- **`AISParallelMapDataset`**: new PyTorch map-style dataset optimized for
  parallel-download of large objects.
- **Cluster log retrieval**: `Cluster.get_node_log()`, `Cluster.get_node_log_archive()`,
  `Cluster.get_cluster_logs()` for fetching AIS daemon logs programmatically.
  New `LogSeverity` and `NodeFilter` enums for filtering.
- **ETL pod logs**: `Etl.logs()` returns `List[ETLNodeLogs]` with per-target ETL pod output.

### Changed

- `read_all()` moved from `ObjectReader` into each content provider as an
  `@abstractmethod` on `BaseContentIterProvider`, returning `Union[bytes, ParallelBuffer]`.
- `ParallelBuffer` and `RingBuffer` extracted into `aistore.sdk.obj.content_iterator.buffer`.

### Fixed

- `ListObjectFlag` enum synced with Go `api/apc/lsmsg.go`:
  renamed `ALL` to `MISSING`, `USE_CACHE` to `NOT_CACHED`; corrected bit position
  for `DONT_HEAD_REMOTE`; added missing flags `BCK_PRESENT`, `NO_RECURSION`,
  `DIFF`, `NO_DIRS`, `IS_S3`, `NBI`.

### Removed

- Removed deprecated methods and parameters (deprecated since v1.10.0–v1.13.0):
  - `Object.get()` — use `Object.get_reader()`
  - `Object.put_content()`, `Object.put_file()`, `Object.append_content()`, `Object.set_custom_props()` — use `Object.get_writer()` equivalents
  - `Client.fetch_object_by_url()` — use `Client.get_object_from_url()`
  - `Client` constructor `retry` (`urllib3.Retry`) parameter — use `retry_config` (`RetryConfig`)
  - `BucketList.get_entries()` — use `BucketList.entries` property

## [1.22.1] - 2026-03-12

### Added

- ETL `FastAPIServer`: manual exponential-backoff retry for streaming direct-put on transient `httpx` errors
  (`ReadError`, `ConnectError`, `RemoteProtocolError`); configurable via `AIS_DIRECT_PUT_RETRIES` (default 3).
- `ETLDirectPutTransientError`: new exception in `aistore.sdk.errors` for retryable streaming direct-put
  failures; carries the destination URL and underlying cause.

## [1.22.0] - 2026-03-11

### Added

- ETL webservers: add `transform_stream(reader, path, etl_args) -> Iterator[bytes]` as a streaming alternative to the buffered `transform()`. Subclasses override one or the other; the base class auto-detects which is implemented. Enables constant-memory transforms for workloads that produce output incrementally (e.g., multi-GB TAR archives). Supported across all three server types (FastAPI, Flask, HTTP multi-threaded) with streaming direct-put pipeline support.
- ETL `FastAPIServer`: configure `AsyncHTTPTransport` with retries to recover from transient connect errors (e.g. DNS `EAI_AGAIN`, connection reset). Tunable via `AIS_DIRECT_PUT_RETRIES` (default 3).

## [1.21.1] - 2026-03-05

### Added

- `AIS_SKIP_VERIFY` environment variable support: when set to `true`, `1`, or `yes`, SSL certificate verification is skipped automatically. Handled centrally in `SessionManager` alongside the existing `AIS_CLIENT_CA`, `AIS_CRT`, and `AIS_CRT_KEY` env vars.
- ETL webservers (`FastAPIServer`, `FlaskServer`, `HTTPMultiThreadedServer`) now read SSL/auth environment variables (`AIS_SKIP_VERIFY`, `AIS_CLIENT_CA`, `AIS_CRT`, `AIS_CRT_KEY`, `AIS_AUTHN_TOKEN`) on startup to configure outbound connections to AIS targets.
- `FastAPIServer`: stream large PUT bodies via async generator (`_iter_chunks`) to avoid single-write SSL failures on multi-gigabyte payloads. Chunk size defaults to 1 MiB and is configurable via `AIS_DIRECT_PUT_CHUNK_SIZE`.

## [1.21.0] - 2026-03-04

### Added

- ETL Webserver: `ETL_DIRECT_FQN` environment variable opt-in for direct file access. When set to `true`, `transform()` receives the object's local filesystem path as `str` instead of loading the file into the Python process's heap
  - Falls back to `bytes` automatically when no FQN is available (e.g., pipeline intermediate stages).
  — Useful for tools like `ffmpeg` that read directly from a file path.

### Changed

- Parallel download switch from `ThreadPoolExecutor` to `ProcessPoolExecutor` with shared memory ring buffer for parallel chunk fetching
- Parallel download auto-enables direct mode (bypasses proxy) to avoid redundant redirect overhead

### Fixed

- ETLServer: Raise file descriptor soft limit to match hard limit on init, fixing `[Errno 24] No file descriptors available` under high-concurrency workloads.

## [1.20.0] - 2026-02-10

### Added
- `Object.head_v2()` API for selective property retrieval (experimental)
- `ObjectAttributesV2` with chunk info (`chunk_count`, `max_chunk_size`)
- Add `Colocation` IntEnum and `colocation` parameter to `Batch` for optimization hints (`Colocation.NONE`, `Colocation.TARGET_AWARE`, `Colocation.TARGET_AND_SHARD_AWARE`)

### Changed

- Dsort integration tests are now conditionally skipped when AIStore is built without the `dsort` build tag.
  - The `/v1/sort` endpoint returns HTTP 501 (Not Implemented) in non-dsort builds.
  - This avoids false failures when running the Python SDK test suite against default AIS builds.
- Parallel download now uses `head_v2` to get optimal chunk size from server

### Fixed

- Parallel download was using 32KiB chunks instead of server-provided chunk size

## [1.19.0] - 2026-01-14

### Deprecated

- Legacy `HEAD(object)` v1 API is now deprecated.
  - Existing calls remain fully supported in 1.19.x.
  - New development should target the chunk-aware Object HEAD v2 API.
  - The v1 path is planned for removal in a future major release.

### Changed

- Sync `AccessAttr` permission constants with Go (`api/apc/access.go`):
  - `ACCESS_RW` now includes `PROMOTE`.
  - Added `ACCESS_BUCKET_ADMIN`, `CLUSTER_ACCESS_RO`, `CLUSTER_ACCESS_RW`.
  - Fixed `ACCESS_RO` to exclude `LIST_BUCKETS` (bucket-level, not cluster-level).

### Fixed

- Fixed HMAC signature mismatch (401 Unauthorized) when using HTTPS with cluster key enabled:
  - Calculate content length for all data types (bytes, strings, file-like objects) to ensure accurate headers.
  - Fix HTTPS manual redirect to avoid SSL EOF errors when sending request data.
  - Include `Content-Length` header in proxy requests for proper URL signing (previously signed with 0, causing 401 on target).

### Added

- `ParallelContentIterProvider`: A new content iterator that fetches object chunks using concurrent HTTP range-reads while yielding them in sequential order.
- `num_workers` parameter in `Object.get_reader()`: When specified, uses `ParallelContentIterProvider` for parallel downloads, improving throughput for large objects.
- `ObjectClient.get_chunk(start, end)`: Fetch a specific byte range of an object. Used for parallel chunk fetching.
- Introduced a new property `requests_list` in the Batch class to return the list of MossIn requests in the batch.

## [1.18.0] - 2025-12-05

### Changed

- Update to Pydantic v2 for all API type parsing.
- Improved type hinting with more explicit Optionals
- **IMPORTANT** - Timeout Behavior Change:
  - Default timeout changed from `(3, 20)` to `None`: When `timeout=None` (the new default), the client now checks environment variables `AIS_CONNECT_TIMEOUT` and `AIS_READ_TIMEOUT`. If not set, it falls back to `(3, 20)`.
  - To disable timeout, use `0` instead of `None`: Previously, `timeout=None` disabled timeouts. Now, use `timeout=0` or `timeout=(0, 0)` to disable all timeouts.
  - Granular timeout control: Use tuples with `0` to disable specific timeouts: `timeout=(0, 20)` disables connect timeout only, `timeout=(5, 0)` disables read timeout only.
  - Environment variables: Added support for environment variables to configure connection timeout (`AIS_CONNECT_TIMEOUT`), read timeout (`AIS_READ_TIMEOUT`), and maximum connection pool size (`AIS_MAX_CONN_POOL`). Set `AIS_CONNECT_TIMEOUT=0` or `AIS_READ_TIMEOUT=0` to disable specific timeouts via environment variables.
- All job wait methods (`Job.wait()`, `Job.wait_for_idle()`, `Job.wait_single_node()`, `Dsort.wait()`) now return unified `WaitResult` dataclass:
  - Check `result.success` to verify success of wait operation.
  - Access error details via `result.error` and completion time via `result.end_time`.
  - Wait timeouts raise `Timeout` exception with detailed debug info.

### Fixed

- `Job.wait_for_idle()` now correctly exits early when job is aborted or failed, instead of falsely timing out.

## [1.17.0] - 2025-10-16

### Added

- Introduced a new `Batch` class to handle GetBatch requests, replacing the old `BatchLoader`.
- Added support for creating batches with multiple objects.

### Changed

- FastAPI ETL Webserver: Introduced HTTP connection pooling via `httpx.Limits` to manage maximum concurrent and keep-alive connections, improving network efficiency under load.
- Transformation handling: Removed per-request thread creation and now rely on the existing executor loop for CPU-bound transforms, reducing thread overhead and overall CPU usage.
- Updated the SDK to utilize `MossIn` and `MossOut` types for better metadata handling (Match Go API 1:1).

### Fixed

- Optimize error message URL parsing regex by bounding quantifiers and removing greedy patterns.

### Removed

- Removed `BatchLoader` and `BatchRequest` classes.


## [1.16.0] - 2025-10-03

### Added

- Add multipart upload support for objects.
  - Introduce `MultipartUpload` class with `create()`, `add_part()`, `complete()`, and `abort()` methods.
  - Add `Object.multipart_upload()` method to create multipart upload sessions.

### Changed

- Move cold get retry delay logic into the tenacity `before_sleep` option and improve logging.
- Add path parameter to `direct_put` methods across all webserver.

## [1.15.2] - 2025-08-18

### Added

- Add functionality to pipeline multiple ETLs.
  - Add `>>` operator for combining multiple ETLs into a pipeline.
  - Introduce `QPARAM_ETL_PIPELINE` constant for ETL pipeline configuration and object inline transformation.
  - Add `etl_pipeline` argument to `Bucket.transform` and `ObjectGroup.transform` APIs.
- Add `pip-system-certs` to common requirements to allow `requests` to access a local certificate in the `uv` virtual environment.

### Removed

- Remove 'deserialize_class' function from ETL Webserver Utils and clean imports.
- Remove `arg_type` environment variable configuration for all ETL web servers, types, and documents; FQN path is now specified per request via query parameter.

## [1.15.1] - 2025-08-11

### Added

- Add ETL pipeline header processing and direct put handling in all ETL webservers.
- Add ETL pipeline processing in WebSocket control message of ETL FastAPI web server.

### Changed
- **BREAKING**: `BatchLoader.get_batch()` no longer takes `extractor` and `decoder` args. Instead, use `return_raw` and `decode_as_stream`.
- Support `ETLConfig.args` parameter in `Object.copy` method.

## [1.15.0] - 2025-07-15

### Added

- Add `ETLRuntimeSpec` class to formalize runtime configuration.
- Add `etl.init(image, command, …)` for simplified setup using only image and command.
- Add archive-extension constant (`EXT_TAR`).
- Add `init_class` method on the Etl client to register and initialize an ETLServer subclass.
- Add support for ETL context manager.
- Add `cont_on_err` option for bucket transform.
- Add `job_id` option to `Etl.view()` method.
- Add support for OS packages in `init_class`.
- Add `MultipartDecoder` class to allow for the parsing of multipart HTTP responses.
- Add `Object.copy()` method with support for `ETLConfig` parameter for copying/transforming a single object.
- Add `Object.copy()` method with support for `latest` and `sync` options.
- Add `BatchLoader`, `BatchRequest`, and `BatchResponseItem` classes for new GetBatch AIStore API.
- Add `ArchiveStreamExtractor` for extraction of archive contents streamed from GetBatch calls.
- Added new internal `BatchObjectRequest` and `BatchResponse` classes to represent metadata.
- Add new `parse_as_stream` field for `MultipartDecoder` allowing for on-the-fly decoding.

### Changed

- **BREAKING**: Update ObjectGroup `copy()` and `archive()` methods to return `List[str]` instead of `str` as these operations can return multiple job IDs (perform operations separately on each job ID).
- Make usage clear for ObjectReader API in `Object.get` deprecation message.
- Make usage clear for ObjectWriter API in `Object.put_content`, `Object.put_file`, `Object.append_content`, and `Object.set_custom_props` deprecation messages.
- Add `list_archive` function to Bucket class: helper method to list entries inside an archived object, with the option to include the archive itself.
- Removes ETL `init_code`.
- Extend `ETLDetails` type with a list of `ETLObjError`.
- Return transformed object size in direct put response for ETL Webservers.
- Update `ObjectReader.__iter__()` to return `Generator[bytes, None, None]`.
- Update `ObjectFileReader.close()` to call `generator.close()` to properly close underlying HTTP streams.
- Rename `Cluster.list_running_etls()` method to `Cluster.list_etls()`, introducing an optional `stage` argument for filtering ETLs by lifecycle stage.
- Rename ETL `Stopped` stage to `Aborted` for improved clarity.

## [1.14.0] - 2025-05-27

### Added

- Add serialization utilities for ETLServer subclasses.
- All APIRequestErrors including AISError and AuthNErrors will contain a field `request` containing the original request (consistent with requests library Errors)
- Added `cold_get_conf` to AISRetryConfig with configurable values for retrying when fetching remote objects.

### Changed

- Update core `RequestClient` to delay retries on `ReadTimeoutError` when waiting on AIS to fetch remote objects into cluster 
- Change default Python ETL web server port from `80` to `8000`.

## [1.13.8] - 2025-05-16

### Changed

- Improve `ObjectFileReader` logging to include the full exception details and traceback when retrying and resuming.
- Update `ObjectFileReader` resume logic to accommodate for limitation w/ the `Streaming-Cold-GET` and read range as to not cause a timeout. 
  - If the remote object is not cached, resuming via stream is not possible and reading must restart from the beginning.
- Replace `id` fields with `name` in `InitSpecETLArgs` and `InitCodeETLArgs` types to align with updated ETL init API spec.

## [1.13.8] - 2025-05-15

### Added

- Add `obj_timeout` and `init_timeout` fields in `InitETLArgs` type to align with updated API fields.
- Add `urllib3.exceptions.TimeoutError` to retry config to handle additional timeout scenarios.

### Changed

- Enhance object file logging to include the object path for better traceability.
- Refactored `ObjectFileReader` to handle additional relevant stream and connection level errors and fixed incorrect assumptions about when certain errors occur (during iteration rather than iterator instantiation).

## [1.13.7] - 2025-05-06

### Added

- Add `path` argument in WebSocket control message of ETL FastAPI web server

## [1.13.6] - 2025-05-02

### Added

- Add `compose_etl_direct_put_url` utility function to compose direct put url from the provided request header

### Changed

- Improve ETL FastAPI web server error reporting by logging the expected message type on `ValueError` during receive.
- Refactor the websocket message pattern in ETL FastAPI web server to always expect a control message as the first incoming message.
- Enhance `transform` method in ETL Webservers to accept `etl_args` for data processing.

## [1.13.5] - 2025-04-25

### Added

- Add `direct put` support in PUT and GET endpoints of all ETL web servers.
- Add `FQN` support in the websocket endpoint of ETL fastapi web server.
- Introduce `glob_id` field on `JobSnapshot` and implement `unpack()` to decode (njoggers, nworkers, chan_full) from the packed integer.

### Changed

- Adjust error handling logic to accommodate updated object naming (see [here](https://github.com/NVIDIA/aistore/blob/main/docs/unicode.md)).
- URL-encode object names when constructing the object paths for the request URLs (see [here](https://github.com/NVIDIA/aistore/blob/main/docs/unicode.md)).
- Add `direct put` support in the websocket endpoint of ETL fastapi web server.
- Remove `rebalance_id` from job JobSnapshot (not used; deprecated)
- Enhanced error handling on closing connection in the websocket endpoint of ETL fastapi web server.
- Rename `JobSnapshot` → `JobSnap` and `AggregatedJobSnapshots` → `AggregatedJobSnaps` for naming consistency

## [1.13.4] - 2025-04-16

### Changed

- **BREAKING:** Update control structure used for bucket-to-bucket copy jobs (`CopyBckMsg` => `TCBckMsg`), which is not backward compatible with cluster version 3.27 and below.

### Added

- Add support for concurrent bucket-to-bucket copy and transformation jobs using the `num_workers` parameter.
- Add support for extension re-mapping in bucket-to-bucket copy jobs using the `ext` parameter.
- Add `direct put` support in the ETL fastapi web server.

## [1.13.3] - 2025-04-11

### Added
- Add `FlaskServer` implementation for ETL transformations.

## [1.13.2] - 2025-04-10

### Changed
- Change `uvicorn` installation to `uvicorn[standard]` for `aistore[etl]` package.

## [1.13.1] - 2025-04-10

### Added
- `props_cached` returns cached properties without triggering a network call.
- Introduce extensible ETL server framework with base and web server implementations.
  - Structured project to promote clean separation between server logic and transformation logic.
  - Add abstract base class `ETLServer` defining the common interface for ETL servers.
      - Includes abstract methods: `transform`, and `start`.  
  - Implement `HTTPMultiThreadedServer`, a multi-threaded server based on `BaseHTTPRequestHandler`.
  - Implement `FastAPIServer` base class for async ETL processing.

### Changed
- `props` accessor ensures object properties are refreshed via HEAD request on every access.

### Removed
- `hrev://` ETL communication type.

## [1.13.0] - 2025-03-24

### Added

- `ResponseHandler` and implementations for AIS and AuthN, to replace static utils.
- `stage` field in `ETLInfo` to represent the ETL lifecycle stage.
- Add `details` method to retrieve detailed job snapshot information across all targets.
- Introduce and type the `AggregatedJobSnapshots` model.
- Implement `get_total_time` to compute the overall job duration.
- Implement `get_all_snapshots` to return a flat list of all job snapshots.
- Introduce `RetryConfig` to standardize HTTP and network retries.

### Changed

- Cluster map with no targets will now result in a `NoTargetError` when using direct object requests.
- `AISError` and `AuthNError` now both inherit from `APIRequestError`.
- `cluster.get_performance()` now returns raw performance data as a `dict` keyed by target IDs, rather than typed aggregator objects.
- `cluster.list_running_etls()` now excludes non-running ETL instances.
- Fix `JobStats` and `JobSnapshot` models.
- Update `pyproject.toml` to enforce a higher minimum version requirement and `common_requirements` to use the latest stable versions for testing.
- Enhanced RequestClient retry strategy for better fault tolerance:
  - Implemented full request retries for `ConnectTimeout`, `RequestsConnectionError`, `ReadTimeout`, `AISRetryableError` and `ChunkedEncodingError`, ensuring retries cover the entire request flow.
  - Optimize `urllib3.Retry` configuration to improve backoff handling.
  - Improve resilience to transient connection failures by refining retry logic.
  - Update dependencies to align with retry behavior improvements.
- Improve retry separation:
  - **HTTP Retry (urllib3.Retry):** Handles HTTP status-based retries.
  - **Network Retry (tenacity):** Manages connection failures and timeouts.
- Update `SessionManager` and `Client` to accept `RetryConfig` for better configurability.
- Allow ETL to override configured `RequestClient` timeout (for that specific ETL request).
- Fix a `NoneType` unpack error in `ObjectClient.get()` by ensuring `byte_range` defaults to `(None, None)` when not explicitly set.
- Set a default request timeout of (3, 20) seconds in AIStore Client.

### Removed

- Typed classes `ClusterPerformance`, `NodeTracker`, `DiskInfo`, `DiskInfoV322`, `NodeCapacityV322`, `NodeCapacity`, `NodeStatsV322`, `NodeStats`, `NodeThroughput`, `NodeLatency`, and `NodeCounter`.

### Deprecated

- The `retry` parameter (`urllib3.Retry`) in `aistore.sdk.Client` is now deprecated. Please use the `retry_config` parameter (`RetryConfig`) instead for configuring retry behavior.

## [1.12.2] - 2025-02-18

### Changed
- Ensure ETL args dictionaries are serialized as strings in query parameters for consistency.

## [1.12.1] - 2025-02-14

### Added

### Changed
- Update ETL argument constant from 'etl_meta' to 'etl_args'.

## [1.12.0] - 2025-02-13

### Added
- Add support for ETL transformation arguments in GET requests for inline objects.
- Introduced `ETLConfig` dataclass to encapsulate ETL-related parameters.
- Add `max_pool_size` parameter to `Client` and `SessionManager`.

### Changed
- Update `get_reader` and `get` methods to support ETLConfig, ensuring consistent handling of ETL metadata.

## [1.11.1] - 2025-02-06

### Changed

- Update project metadata and improve descriptions in `pyproject.toml`.

- Update supported ETL Python runtime versions, and set the default runtime version to Python 3.13.

## [1.11.0] - 2025-02-06

### Added

- Support for OCI (`oci://`) as remote backend.

- Support for reading objects directly from the targets (bypassing proxy and LB) via the `direct=True` parameter in `object.get_reader()`.

### Changed

- Improvements to Python SDK error handling.

- Replaced `datetime.fromisoformat` usage and custom parsing logic with `dateutil.parser.isoparse` for more consistent and robust ISO timestamp handling in `job.py`.

## [1.10.1] - 2024-12-20

### Added

- `ext` parameter in `object_group.transform(...)` enables replacing file extensions during multi-object transformations (e.g., `{"jpg": "txt"}`).

### Changed

- `ObjectFile` renamed to `ObjectFileReader` ensures naming consistency with later-introduced `ObjectFileWriter`; related errors updated accordingly (e.g., `ObjectFileMaxResumeError` → `ObjectFileReaderMaxResumeError`).

- `ContentIterator` now properly respects the associated `ObjectClient`’s existing byte-range settings, ensuring correct iteration in `ObjectReader.__iter__()` and `ObjectFile`.

### Deprecated

- `client.fetch_object_by_url` renamed to `client.get_object_by_url` to better reflect its purpose (calling `client.fetch_object_by_url` now emits a deprecation warning and will be removed in a future release).

## [1.10.0] - 2024-12-03

### Added

- `ObjectWriter` provides a structured interface akin to the `ObjectReader` class for writing objects (e.g., `put_content`, `append_content`, `set_custom_props`), instantiable via `object.get_writer`.

- `ObjectFileWriter` is a file-like writer instantiable via `ObjectWriter.as_file(...)`.

### Deprecated

- `object.get` renamed to `object.get_reader` for clarity and consistency (calling `object.get` now emits a deprecation warning and will be removed in a future release).

- `Object` write methods (`object.put_content`, `object.put_file`, `object.append_content`, `object.set_custom_props`) are now deprecated in favor of `ObjectWriter` equivalents (calling the deprecated methods now emits deprecation warnings and will be removed in a future release).

## [1.9.2] - 2024-10-25

### Changed

- `pyproject.toml` to require `pydantic>=1.10.17` and switched all references to `pydantic.v1`, ensuring compatibility with both v1 and v2 while preserving existing v1 features (see [here](https://docs.pydantic.dev/latest/migration/#using-pydantic-v1-features-in-a-v1v2-environment)).
