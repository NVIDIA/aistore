# Changelog

All notable changes to the AIStore Python SDK project are documented in this file.

We structure this changelog in accordance with [Keep a Changelog](https://keepachangelog.com/) guidelines, and this project follows [Semantic Versioning](https://semver.org/).

## Unreleased

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
