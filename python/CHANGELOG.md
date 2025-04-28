# Changelog

All notable changes to the AIStore Python SDK project are documented in this file.

We structure this changelog in accordance with [Keep a Changelog](https://keepachangelog.com/) guidelines, and this project follows [Semantic Versioning](https://semver.org/).

---

## Unreleased

### Added

### Changed

- Improve ETL FastAPI web server error reporting by logging the expected message type on `ValueError` during receive.

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
