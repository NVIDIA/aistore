# Changelog

All notable changes to the AIStore Python SDK project are documented in this file.

We structure this changelog in accordance with [Keep a Changelog](https://keepachangelog.com/) guidelines, and this project follows [Semantic Versioning](https://semver.org/).

---

## Unreleased

### Added

- `stage` field in `ETLInfo` to represent the ETL lifecycle stage.

### Changed

- `cluster.get_performance()` now returns raw performance data as a `dict` keyed by target IDs, rather than typed aggregator objects.
- `cluster.list_running_etls()` now excludes non-running ETL instances.
- fix `JobStats` and `JobSnapshot` models.

### Removed

- Typed classes `ClusterPerformance`, `NodeTracker`, `DiskInfo`, `DiskInfoV322`, `NodeCapacityV322`, `NodeCapacity`, `NodeStatsV322`, `NodeStats`, `NodeThroughput`, `NodeLatency`, and `NodeCounter`.

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
