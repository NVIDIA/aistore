AIStore natively supports four archive/serialization formats across all APIs, batch jobs, and functional extensions: **TAR**, **TGZ** (TAR.GZ), **TAR.LZ4**, and **ZIP**.

## Motivation

Archives address the [small-file problem](https://scholar.google.com/scholar?hl=en&as_sdt=0%2C5&q=%22small+file+problem%22) - performance degradation from random access to very large datasets containing many small files.

> To qualify "very large" and "small-file" - the range of the numbers we usually see in the field include datasets containing 10+ million files with sizes ranging from 1K to 100K.

AIStore’s implementation allows **unmodified** clients and applications to work efficiently with archived datasets.

Key benefits:

- Improved I/O performance via reduced metadata lookups and network roundtrips
- Seamless integration with existing, unmodified workflows
- Implicit dataset backup: each archive acts as a self-contained, immutable copy of the original files

> In addition to performance, sharded datasets provide a natural form of **dataset backup**: each shard is a self-contained, immutable representation of its original files, making it easy to replicate, snapshot, or version datasets without additional tooling.

## Supported Formats

* **TAR** (`.tar`) - Unix archive format (since 1979) supporting USTAR, PAX, and GNU TAR variants
* **TGZ** (`.tgz`, `.tar.gz`) - TAR with gzip compression
* **TAR.LZ4** (`.tar.lz4`) - TAR with lz4 compression
* **ZIP** (`.zip`) - [PKWARE ZIP](https://www.pkware.com/appnote) format (since 1989)

## Operations

AIStore can natively **read**, **write**, **append**¹, and **list** archives. Operations include:

- Regular GET and PUT requests:
  - [Go API](https://github.com/NVIDIA/aistore/blob/main/api/object.go) - see "ArchPath" parameter
  - [Python SDK](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/obj/object.py) - ditto
  - [Python SDK/Archive](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/archive_config.py) - see archive-related config
- [**get-batch**](/docs/get_batch.md) - efficient multi-object/multi-file retrieval
- **list-objects** - "opens" archives and includes contained pathnames in results
- **[dsort](/docs/cli/dsort.md)** - distributed archive creation and transformation
- **[aisloader](/docs/aisloader.md)** - benchmarking with archive workloads
- Concurrent multi-object transactions for bulk archive generation from [selected](/docs/batch.md) objects

**Default format**: TAR is the system default when serialization format is unspecified.

---
¹ **APPEND** is supported for [TAR format only](https://aistore.nvidia.com/blog/2021/08/10/tar-append). Other formats (ZIP, TGZ, TAR.LZ4) were not designed for true append operations - only extract-all-recreate emulation, which significantly impacts performance.

## See also

* [CLI: archive](/docs/cli/archive.md)
* [aisloader: archive](/docs/aisloader.md#command-line-options)
* [Initial Sharding Tool (`ishard`)](https://github.com/NVIDIA/aistore/blob/main/cmd/ishard/README.md)
* [Distributed Shuffle](/docs/cli/dsort.md)
* [Get-Batch](/docs/get_batch.md)
