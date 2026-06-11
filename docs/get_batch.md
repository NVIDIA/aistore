# GetBatch: Distributed Multi-Object Retrieval

> **Paper:** [GetBatch: Distributed Multi-Object Retrieval for ML Data Loading](https://arxiv.org/abs/2602.22434) - implementation, benchmarks, and discussion.

GetBatch is AIStore's high-performance API for retrieving multiple objects and/or archived files in a single request. Behind the scenes, GetBatch assembles the requested data from across the cluster and delivers the result as a continuous serialized stream.

Regardless of retrieval source (in-cluster objects, remote objects, or shard extractions), GetBatch always preserves the exact ordering of request entries in both streaming and buffered modes.

Other supported capabilities include:

- Fetching thousands of objects in strict user-specified order
- Extracting specific files from distributed shard archives (TAR, TAR.GZ, TAR.LZ4, ZIP)
- Cross-bucket retrieval in a single request
- Graceful handling of missing data
- Streaming or buffered delivery

> GetBatch is implemented by the [eXtended Action](/docs/terminology.md#xaction) (job). Internally, the job is codenamed `x-moss`. The respective API endpoint is: `/v1/ml/moss`.

> Note: buffered mode always returns both metadata (that describes the output) and the resulting serialized archive containing all requested data items.

> Note: for TAR.GZ, both .tgz and .tar.gz are accepted (and interchangeable) aliases - both denote the same exact format.

**A Note on HTTP Semantics**

GetBatch uses **HTTP GET with a JSON request body**, which is:

- Permitted by [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) - HTTP semantics allow message bodies in GET requests.
- Necessary for this API, as the list of requested objects can contain thousands of entries that would exceed URL length limits.
- Semantically correct - the operation is idempotent (pure data retrieval with no server-side state changes).

The rest of this document is structured as follows:

**Table of Contents**

* [Supported APIs](#supported-apis)
* [Terminology](#terminology)
* [When to Use GetBatch](#when-to-use-getbatch)
* [When NOT to Use GetBatch](#when-not-to-use-getbatch)
* [Go API Structure](#go-api-structure)
* [Python Integrations](#python-integrations)
* [Usage Examples](#usage-examples)
* [Direct Access to Archived Data](#direct-access-to-archived-data)
* [Performance and Access Patterns](#performance-and-access-patterns)
* [Performance Tips](#performance-tips)
* [Error Handling](#error-handling)
* [Configuration](#configuration)
* [Output Formats](#output-formats)
* [Naming Conventions](#naming-conventions)
* [Monitoring & Observability](#monitoring-observability)
* [Advanced Use Cases](#advanced-use-cases)
* [Limitations & Future Work](#limitations-future-work)
* [References](#references)

## Supported APIs

GetBatch is typically called from:

1. Go services via the [`api/ml.go`](https://github.com/NVIDIA/aistore/blob/main/api/ml.go) client bindings, and
2. Python via the [AIStore SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk/batch), and
3. Third-party tooling such as [Lhotse](https://github.com/lhotse-speech/lhotse/blob/master/lhotse/ais/batch_loader.py).

The respective Go and Python-based usage examples follow below, in the sections that include:

* [Go API Structure](#go-api-structure)
* [Python Integrations](#python-integrations)
* [Advanced Use Cases](#advanced-use-cases)

## Terminology

| Term | Description |
|------|-------------|
| **Designated Target (DT)** | Randomly selected AIS target that coordinates and assembles the get-batch response |
| **Work Item (WI)** | A single get-batch request being processed; implementation-wise, each request becomes one work item |
| **Shard** | An archive file (TAR/ZIP/TGZ/LZ4) stored in (or accessible by) the cluster and containing multiple files, typically used for dataset distribution |
| **Soft Error** | Recoverable error (missing object, transient network issue) that doesn't fail the entire request when `coer: true` |
| **Hard Error** | Unrecoverable failure that terminates the work item (e.g., 429 rejection, fatal errors) |
| **Get-From-Neighbor (GFN)** | Fallback mechanism to retrieve data from peer targets when primary location fails |
| **RxWait** | Time spent waiting to receive data from peer targets (coordination/network delay) |

## When to Use GetBatch

**ML Training Pipelines**
- Loading training data in deterministic order (reproducible epochs)
- Fetching 1000s-100Ks of objects per training batch
- Consuming sharded datasets where each shard contains many samples

**Distributed Shard Processing**
- Extracting specific files from archives distributed across the cluster
- Processing subsets of large TAR/ZIP collections
- Parallel extraction from 1000s of shards

**Ordered Batch Retrieval**
- Any workflow requiring strict ordering guarantees
- Sequential processing pipelines
- Deterministic data sampling

## When NOT to Use GetBatch

- Single objects => Use regular GET
- Small batches (<16 entries) => Overhead not justified; use parallel GETs
- Latency-critical single-item random access => Use regular GET or a purpose-built lookup path; GetBatch is optimized for amortized multi-entry retrieval.

> Whether a batch is "small" depends on the workload and cluster. Any specific numbers in this document should be treated as rules of thumb, not as hard limits.

---

## Go API Structure

### Request: [`apc.MossReq`](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go)

```json
{
  "mime": ".tar",           // Output format: .tar, .tgz, .zip, .tar.lz4
  "in": [                   // Array of items to retrieve
    {
      "objname": "shard-0000.tar",
      "archpath": "image_42.jpg"  // Optional: extract specific file from archive
    },
    {
      "objname": "dataset/file-0001.bin",
      "bucket": "other-bucket"     // Optional: override default bucket
    }
  ],
  "coer": true,             // Continue on error (missing items => __404__/)
  "onob": false,            // Name in TAR: false = bucket/object, true = object only
  "strm": true              // Stream output (vs buffered multipart)
}
```

**Field Details:**

| Field | Type | Description |
|-------|------|-------------|
| `mime` | string | Output format: `.tar` (default), `.tgz`, `.zip`, `.tar.lz4` |
| `in`   | [][apc.MossIn](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go) | List of objects/files to retrieve (order preserved) |
| `coer` | bool | Continue on error: `true` = include missing items under `__404__/`, `false` = fail on first missing |
| `onob` | bool | Output naming: `false` = `bucket/object`, `true` = `object` only |
| `strm` | bool | Streaming mode: `true` = stream as data arrives, `false` = buffer then send multipart response |

### Request Entry: [`apc.MossIn`](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go)

Each entry in the `in` array can specify:

| Field | Type | Description |
|-------|------|-------------|
| `objname` | string | **Required.** Object name to retrieve |
| `bucket` | string | Override default bucket (enables cross-bucket requests) |
| `provider` | string | Provider for this object (e.g., "s3", "ais", "gcp"). If omitted, defaults to the bucket's. |
| `uname` | string | Fully-qualified bucket specification, including bucket name, provider and namespace. |
| `archpath` | string | Path to file within **input** archive (for TAR/ZIP/TGZ/LZ4 shards) |
| `opaque` | []byte | Opaque user identifier (passed through to response to implement client-side logic of any kind) |
| `start` | int64 | Range read start offset. When `archpath` is empty, the range applies to the object bytes as stored (a raw byte range); when `archpath` is set, it applies to the extracted archived file. A non-zero `start` requires a `length`. |
| `length` | int64 | Range read length: `L>0` reads exactly `L` bytes; `-1` reads from `start` to the end (open-ended); `(start=0, length=0)` is the whole object. For a fixed-length range, `MossOut.size` equals `length`. |

### Response: [`apc.MossResp`](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go)

**Streaming mode (`strm: true`):**
- Single HTTP response body containing TAR stream
- Files appear in exact order requested
- No separate metadata structure

**Buffered mode (`strm: false`):**
- Multipart HTTP response with two parts:
  1. **Metadata part** (`MossResp` JSON)
  2. **Data part** (TAR archive)

```json
{
  "uuid": "xaction-id",
  "out": [
    {
      "objname": "shard-0000.tar",
      "archpath": "image_42.jpg",
      "bucket": "training-data",
      "provider": "ais",
      "size": 524288,
      "err_msg": ""
    },
    {
      "objname": "missing-file.bin",
      "size": 0,
      "err_msg": "object does not exist"
    }
  ]
}
```

**Response Entry: `apc.MossOut`**

| Field | Type | Description |
|-------|------|-------------|
| `objname` | string | Object name (matches request) |
| `archpath` | string | Archive path if extracted from shard |
| `bucket` | string | Bucket name |
| `provider` | string | Provider |
| `size` | int64 | Actual bytes delivered (0 if missing) |
| `err_msg` | string | Error message if item failed (with `coer: true`) |
| `opaque` | []byte | User metadata from request |

---

## Python Integrations

GetBatch also offers robust integration with Python data pipelines, with official support in both the AIStore Python SDK and third-party libraries:

### 1. AIStore Python SDK

The [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk) provides a [`Batch`](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk/batch) class that wraps the `/v1/ml/moss` endpoint with a Pythonic fluent API.

**Key features:**
- Pydantic models (`MossReq`, `MossIn`, `MossOut`) that mirror Go structs exactly
- Fluent interface for building batch requests
- Automatic TAR stream extraction
- Support for archpath (shard extraction), opaque metadata, and cross-bucket requests

The SDK mirrors Go structures, with minor naming conventions:

| Concept | Go API | Python SDK |
|--------|--------|-------------|
| Output archive format | `MossReq.mime` | `output_format=".tar"` |
| Continue-on-error | `MossReq.coer` | `cont_on_err=True` |
| Streaming mode | `MossReq.strm` | `streaming_get=True` |
| Archive subpath | `MossIn.archpath` | `archpath=` |
| Opaque metadata | `MossIn.opaque` | `opaque=` |
| Object name | `objname` | `obj_name` |

> This mapping helps translate examples between Go and Python.

**Basic usage:**
```python
from aistore.sdk import Client

client = Client("http://ais-gateway")
bucket = client.bucket("training-data")

# Simple batch: list of object names
batch = client.batch(["file1.bin", "file2.bin"], bucket)
for obj_info, content in batch.get():
    print(f"Got {obj_info.obj_name}: {len(content)} bytes")

# Advanced: shard extraction with tracking
batch = client.batch(bucket=bucket)
# Extract specific files from archives
batch.add("shard-0000.tar", archpath="images/photo.jpg")
# Add opaque data for tracking/correlation
batch.add("shard-0001.tar", archpath="images/photo.jpg", opaque=b"batch-id-42") 

# Stream results
for obj_info, content in batch.get():
    if not obj_info.err_msg:  # Check for errors
        process(content)
```

**Batch class methods:**
- `add(obj, archpath=None, opaque=None, start=None, length=None)` - Add object with advanced parameters
- `get(raw=False, decode_as_stream=False, clear_batch=True)` - Execute and return generator of `(MossOut, bytes)` tuples
- `clear()` - Clear batch for reuse

**Response structure:**
- **Streaming mode** (`streaming_get=True`, default): Returns TAR stream, `MossOut` metadata inferred from request
- **Multipart mode** (`streaming_get=False`): Returns server-validated `MossOut` with actual sizes, errors

See [Python SDK Batch API](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk/batch) for complete documentation.

### 2. Lhotse Integration

[Lhotse](https://github.com/lhotse-speech/lhotse) is a speech/audio data toolkit used by NVIDIA NeMo and other frameworks. It includes native AIStore support via [`AISBatchLoader`](https://github.com/lhotse-speech/lhotse/blob/master/lhotse/ais/batch_loader.py).

**How it works:**

1. **CutSet Manifests** - Lhotse manages audio/feature manifests with AIStore URLs
2. **Batch Loading** - `AISBatchLoader` collects all URLs from a CutSet batch
3. **GetBatch Execution** - Issues single batch request via AIStore Python SDK
4. **Archive Extraction** - Automatically extracts files from sharded archives (TAR/TGZ)
5. **In-Memory Injection** - Returns CutSet with data loaded into memory

**Usage:**
```python
from lhotse.ais import AISBatchLoader

# Create AIStore BatchLoader
ais_batch_loader = AISBatchLoader()

# Load entire dev cutset from AIStore
loaded_cut_set = ais_batch_loader(cut_set)

# Pass the loaded cut-set to DataLoaders
```

**See also:**

A complete, runnable example for batch loading audio from AIStore with Lhotse is available here:

* https://github.com/lhotse-speech/lhotse/blob/master/examples/06-train-ais-batch.ipynb

**Archive extraction example:**

Lhotse URLs like `ais://mybucket/shard-0000.tar.gz/audio/sample_42.wav` automatically:
- Split into object (`shard-0000.tar.gz`) + archpath (`audio/sample_42.wav`)
- Send to GetBatch with `archpath` parameter
- AIStore extracts the file server-side
- Returns raw audio bytes directly to training loop

**Architecture:**
```
Lhotse CutSet => AISBatchLoader => AIStore Python SDK => GetBatch API => Training Loop
```

**Key benefits:**
- **Single batch request** instead of N individual GETs
- **Server-side extraction** from sharded archives
- **Deterministic ordering** for reproducible training
- **Zero client-side decompression** overhead

See also:
- [AIStore in NeMo workflows](https://docs.nvidia.com/nemo-framework/) (data loading section)

### 3. NeMo Framework Integration

[NVIDIA NeMo](https://github.com/NVIDIA/NeMo) is an end-to-end platform for building and training state-of-the-art AI models, including Automatic Speech Recognition (ASR), Natural Language Processing (NLP), and Text-to-Speech (TTS).

GetBatch is now integrated into the Lhotse-based [ASR dataloader](https://github.com/NVIDIA-NeMo/NeMo/commit/914c9ce7a54de813e04226dd44277fe159c07a75). Instead of fetching individual audio files from AIStore during training, the dataloader now batches all required samples for an epoch or mini-batch into a single GetBatch request. This reduces network overhead and improves data loading throughput for large-scale ASR training.

**Enabling GetBatch:**

```bash
export USE_AIS_GET_BATCH=true
```

A single environment variable activates batch loading for ASR training pipelines using Lhotse+AIStore. No code changes required.

**How it works:**
1. Dataloader collects all audio file paths needed for the current batch
2. Issues single GetBatch request to AIStore (replaces N individual GETs)
3. AIStore returns TAR archive with all samples in order
4. Dataloader extracts and feeds samples to the training loop

This same pattern can be integrated in other NeMo training pipelines (NLP, TTS, multimodal) where datasets are stored in AIStore, providing similar performance benefits for data-intensive workloads.

**See also:**
- [NeMo ASR documentation](https://docs.nvidia.com/nemo-framework/user-guide/latest/nemotoolkit/asr/intro.html)

### 4. PyTorch Integration

The [AIStore PyTorch Plugin](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch) provides `AISBatchIterDataset`, an iterable-style dataset that uses GetBatch API for efficient multi-worker data loading with automatic batching and streaming support.

---

## Usage Examples

> In the `curl` examples below, replace `aistore-gateway` and the bucket/object names with your own.

### Example 1: Retrieve Plain Objects

```console
curl -L -X GET "http://aistore-gateway/v1/ml/moss/my-bucket?provider=ais" \
  -H "Content-Type: application/json" \
  -d '{
    "mime": ".tar",
    "in": [
      {"objname": "file-0001.bin"},
      {"objname": "file-0002.bin"},
      {"objname": "file-0003.bin"}
    ],
    "strm": true
  }' --output batch.tar
```

Result: `batch.tar` containing:
```
my-bucket/file-0001.bin
my-bucket/file-0002.bin
my-bucket/file-0003.bin
```

### Example 2: Extract Files from Shards

```console
# Extract image_42.jpg from each distributed shard
curl -L -X GET "http://aistore-gateway/v1/ml/moss/shards?provider=ais" \
  -H "Content-Type: application/json" \
  -d '{
    "mime": ".tar",
    "in": [
      {"objname": "shard-0000.tar", "archpath": "image_42.jpg"},
      {"objname": "shard-0001.tar", "archpath": "image_42.jpg"},
      {"objname": "shard-0002.tar", "archpath": "image_42.jpg"}
    ],
    "onob": true,
    "strm": true
  }' --output extracted.tar
```

Result: `extracted.tar` containing:
```
shard-0000.tar/image_42.jpg
shard-0001.tar/image_42.jpg
shard-0002.tar/image_42.jpg
```

### Example 3: Cross-Bucket Retrieval

> The request bucket (`default-bucket` in URL) is used when bucket is omitted in an in entry.

```console
curl -L -X GET "http://aistore-gateway/v1/ml/moss/default-bucket?provider=ais" \
  -H "Content-Type: application/json" \
  -d '{
    "in": [
      {"objname": "config.json", "bucket": "configs"},
      {"objname": "model.pt", "bucket": "models"},
      {"objname": "data.csv", "bucket": "datasets"}
    ],
    "strm": true
  }' --output multi-bucket.tar
```

Result: TAR containing objects from three different buckets in one request.

### Example 4: Handle Missing Data Gracefully

```console
curl -L -X GET "http://aistore-gateway/v1/ml/moss/training?provider=ais" \
  -H "Content-Type: application/json" \
  -d '{
    "in": [
      {"objname": "exists-1.bin"},
      {"objname": "missing.bin"},
      {"objname": "exists-2.bin"}
    ],
    "coer": true,
    "strm": false
  }' --output result.tar
```

Response metadata shows which items failed:
```json
{
  "out": [
    {"objname": "exists-1.bin", "size": 1048576, "err_msg": ""},
    {"objname": "missing.bin", "size": 0, "err_msg": "object does not exist"},
    {"objname": "exists-2.bin", "size": 2097152, "err_msg": ""}
  ]
}
```

TAR contains:
```
training/exists-1.bin          (1 MB)
__404__/training/missing.bin   (0 bytes)
training/exists-2.bin          (2 MB)
```

---

### Example 5: Range Reads (Partial Object Retrieval)

Per-entry `start`/`length` fields request a byte range instead of the whole object. When
`archpath` is empty the range applies to the object bytes as stored (a raw byte range); when
`archpath` is set the range applies to the *extracted* archived file.

Three forms are supported:
- `(start=0, length=0)` - the entire object/file (no range)
- `(start=N, length=L)` with `L>0` - exactly `L` bytes starting at offset `N` (`MossOut.size == L`)
- `(start=N, length=-1)` - open-ended: from offset `N` to the end of the object/file

A non-zero `start` requires a `length` (use `-1` to read to the end). Ranges that fall outside
the object's bounds are reported as `ErrRangeNotSatisfiable`.

```console
curl -L -X GET "http://aistore-gateway/v1/ml/moss/my-bucket?provider=ais" \
  -H "Content-Type: application/json" \
  -d '{
    "in": [
      {"objname": "large.bin", "start": 0, "length": 1024},
      {"objname": "large.bin", "start": 4096, "length": 1024},
      {"objname": "large.bin", "start": 4096, "length": -1},
      {"objname": "shard.tar", "archpath": "sample.json", "start": 0, "length": 256}
    ],
    "strm": true
  }' --output result.tar
```

With the Python SDK:

```python
batch = client.batch(bucket=bucket, streaming_get=False)
batch.add("large.bin", start=0, length=1024)         # first 1 KiB
batch.add("large.bin", start=4096, length=1024)      # 1 KiB at offset 4096
batch.add("large.bin", start=4096, length=-1)        # open-ended: offset 4096 to EOF
batch.add("shard.tar", archpath="sample.json", start=0, length=256)  # range over archived file

for obj_info, data in batch.get():
    assert len(data) == obj_info.size
```

---

## Direct Access to Archived Data

GetBatch can avoid sequential scans when the request provides enough information to locate the requested bytes directly.

This matters most for large sharded datasets, where each shard is an archive such as TAR and each training sample is stored as an archived file inside the shard.

There are two related cases:

### Indexed archive extraction

When archive shards are indexed, AIS can resolve an archived file by name and use the stored offset metadata to access it directly.

For example, a request entry with both `objname` and `archpath`:

```json
{
  "objname": "shard-000042.tar",
  "archpath": "samples/000123.bin"
}
```

selects `samples/000123.bin` inside `shard-000042.tar`.

With a shard index available, AIS does not need to scan the archive sequentially from the beginning to find the requested file.

### Range reads

GetBatch also supports byte ranges via `start` and `length`.

The range is interpreted relative to the thing being requested:

* If `archpath` is empty, the range applies to the raw object bytes as stored in AIS.
* If `archpath` is set, AIS first resolves the archived file named by `archpath`, and the range then applies to that archived file.

In other words:

```text
objname only:
    range applies to object bytes

objname + archpath:
    archpath selects the archived file
    range applies inside that archived file
```

For example:

```json
{
  "objname": "shard-000042.tar",
  "archpath": "samples/000123.bin",
  "start": 1048576,
  "length": 262144
}
```

returns 256 KiB starting at offset 1 MiB within `samples/000123.bin`, not within `shard-000042.tar`.

### Why this matters

Without direct access, retrieving a file from a large archive may require scanning entries from the beginning until the requested file is found.

With indexing and/or client-provided range metadata, GetBatch can seek much closer to the requested data and read only the required bytes.

For large shards, especially when requested files are located deep inside the archive, this can improve retrieval latency by orders of magnitude.


---

## Performance and Access Patterns

> **Note:** Actual throughput will vary significantly based on object sizes, network topology, storage backend, CPU capability, and cluster configuration. Numbers below are purely indicative ranges rather than guaranteed performance targets.

| Workload | Typical Throughput | Primary Bottleneck |
|----------|-------------------|-------------------|
| Plain objects (located at [DT](#terminology)) | 50-150K objects/sec/target | Disk I/O, network |
| Plain objects (located in other storage nodes) | 10-50K objects/sec/target | Peer network, coordination |
| Archived files (located at [DT](#terminology)) | 5-20K files/sec/target | CPU (extraction), I/O |
| Archived files (remote shards) | 2-10K files/sec/target | Peer extraction + network |

**Note:** Archived file extraction from compressed formats is CPU-intensive. A single target extracting from 1000s of TAR/ZIP shards will see significant CPU load.

### Latency Components

**First-byte latency:** 50-500ms (can vary based on cluster size and load)
- Designated Target (DT) selection
- Peer coordination
- First data arrival

**Streaming throughput:** Wire-speed after first byte
- Limited by network bandwidth or disk I/O
- No additional per-object overhead once streaming starts

### Shard index fast path

See [Direct Access to Archived Data](#direct-access-to-archived-data) for the access semantics.

To prepare a bucket, use the common `ais start JOB` command or `ais bucket shard-index` - the latter with additional capabilities:

```console
ais bucket shard-index --help
NAME:
   ais bucket shard-index - Manage TAR shard indexes for fast random access into archives.
     Subcommands:
     - build  - build a shard index for each TAR object in a bucket;
     - summary - summarize TAR objects and their shard-index coverage.

USAGE:
   ais bucket shard-index command [arguments...]  [command options]

COMMANDS:
   build  Build a shard index for each TAR object in a bucket (for fast random access into archives).
          Non-TAR objects are skipped; stale indexes (e.g., after re-upload) are automatically re-indexed.
   e.g.:
     - 'ais bucket shard-index build ais://nnn'                   - index all TAR shards in 'ais://nnn';
     - 'ais bucket shard-index build ais://nnn --prefix shards/'  - only index TAR shards under 'shards/';
     - 'ais bucket shard-index build ais://nnn --num-workers 16'  - run with 16 concurrent workers;
     - 'ais bucket shard-index build ais://nnn --skip-verify'     - fast re-run: trust existing indexes without re-verifying;
     - 'ais bucket shard-index build ais://nnn --wait'            - start and wait for the job to finish.

   summary  Summarize TAR objects in a bucket and their shard-index coverage.
            Only local in-cluster objects are summarized.
   e.g.:
     - 'ais bucket shard-index summary ais://nnn'                  - summarize all TAR shards in 'ais://nnn';
     - 'ais bucket shard-index summary ais://nnn --prefix shards/' - summarize only TAR shards under 'shards/';
     - 'ais bucket shard-index summary ais://nnn/shards/'          - same as above;
     - 'ais bucket shard-index summary ais://nnn --refresh 1s'     - print periodic progress while summarizing.
```

- New shards are indexed on first read or via the indexing xaction.
- Indexes live in the system bucket called `ais://.sys-shardidx`.
- Indexes are removed automatically when the underlying shard object(s) are deleted or updated.

For batches that fan out across many archived files in different shards, this changes per-file extraction from O(archive size) to O(1) + read.

### Memory & Resource Usage

**Memory:** Bounded by DT capacity + load-based throttling
- System monitors memory pressure
- Automatically throttles or rejects (429) new requests under stress
- See [Monitoring GetBatch](/docs/monitoring-get-batch.md) for observability

**CPU:** Varies by workload
- Plain objects: minimal CPU (file I/O bound)
- Compressed archives: moderate CPU (decompression)
- Many small files: higher CPU (archive parsing overhead)

---

## Performance Tips

For best performance, try to make each GetBatch request easy to assemble and cheap to read.

### Use direct access when possible

For large archive shards, avoid repeated sequential scans. See [Direct Access to Archived Data](#direct-access-to-archived-data).

Use shard indexes when selecting archived files by `archpath`. Use range reads when the client already knows the byte offset and length of the required data.

### Use colocation hints when the client knows the layout

By default, GetBatch assumes that requested objects are distributed uniformly across the cluster.

If the client knows that the batch is concentrated on a small number of targets or shards, set the colocation hint accordingly:

* `0`: no colocation hint; suitable for uniformly distributed object names.
* `1`: target-aware; use when requested objects are likely collocated on a small number of targets.
* `2`: target- and shard-aware; use when requested `archpath` entries are likely concentrated in a small number of archive shards.

The hint helps AIS to select a better designated target and can reduce cross-cluster data movement. Level `2` can also improve archive handle reuse when many requested files come from the same shard.

### Prefer larger batches, but avoid unbounded requests

GetBatch is designed to amortize request overhead across many entries. Very small batches may be faster as regular concurrent GETs, while very large batches can increase memory pressure, queueing, and retry cost.

Treat batch size as workload-dependent and validate it under realistic cluster load.

### Prefer uncompressed TAR for maximum throughput

Compressed archive formats reduce network bytes but add CPU overhead for decompression.

When storage and network bandwidth are not the bottleneck, plain `.tar` is usually the best output format for maximum throughput.

### Prefetch or cold-GET remote data

GetBatch operates on in-cluster data. For remote buckets, pre-load or prefetch before running.

---

## Error Handling

### Strict Mode (`coer: false`)

**Behavior:** First error aborts entire request

**Use when:**
- Data completeness is critical
- Prefer fail-fast over partial results
- Small batches where retry cost is low

**Error response:** HTTP 4xx/5xx, no partial data

### Graceful Mode (`coer: true`) ✅ **Recommended**

**Behavior:** Continue processing, mark missing items

**Use when:**
- Large batches (1000s+ items)
- Some missing data is acceptable
- Want to maximize throughput despite occasional 404s

**Missing items:**
- Appear in TAR under `__404__/bucket/object` with size=0
- Metadata includes `err_msg` describing failure
- Extracting the TAR shows all missing items grouped under `__404__/`

**Soft error limit:** Configurable per work item (default: 6)
- Prevents cascading failures
- Aborts work item after N transient errors
- See Configuration section below

---

## Configuration

### `get_batch.max_soft_errs` (default: 6)

Maximum transient errors per work item before aborting.

**When to increase:**
- Large batches with expected missing data
- Tolerate more GFN (get-from-neighbor) fallbacks
- Unstable network environments

**When to decrease:**
- Strict availability requirements
- Fail faster on systemic issues

```console
ais config cluster get_batch.max_soft_errs=10
```

### `get_batch.warmup_workers` (default: 2)

Pagecache warming pool size (best-effort read-ahead).

**When to increase:**
- Fast NVMe storage
- Reduce first-access latency
- CPU/memory headroom available

**When to disable (set to -1):**
- High memory pressure
- Slow disks where read-ahead adds no value
- CPU-constrained environments

```console
ais config cluster get_batch.warmup_workers=4
```

To disable warmup/look-ahead operation:

```console
ais config cluster get_batch.warmup_workers=-1
```

---

## Output Formats

GetBatch supports multiple archive formats via the `mime` field:

| Format | Extension | Use Case |
|--------|-----------|----------|
| TAR | `.tar` | Default, fastest, no compression |
| TAR+GZIP | `.tgz` or `.tar.gz` | Compressed, slower but smaller |
| ZIP | `.zip` | Windows-compatible, moderate compression |
| TAR+LZ4 | `.tar.lz4` | Fast compression, good balance |

**Recommendation:** Use `.tar` for maximum throughput unless network bandwidth is constrained.

---

## Naming Conventions

### Default Naming (`onob: false`)

Files in output TAR include bucket prefix:
```
bucket-name/object-1
bucket-name/object-2
other-bucket/object-3
```

### Object-Only Naming (`onob: true`)

Files in output TAR omit bucket:
```
object-1
object-2
object-3
```

### Archived Files

When extracting from shards with `archpath`:

**Default (`onob: false`):**
```
bucket/shard-0000.tar/image_42.jpg
bucket/shard-0001.tar/image_42.jpg
```

**Object-only (`onob: true`):**
```
shard-0000.tar/image_42.jpg
shard-0001.tar/image_42.jpg
```

---

## Monitoring & Observability

GetBatch exposes Prometheus metrics for:
- Throughput (objects vs archived files)
- Resource pressure (throttling, RxWait stalls)
- Error rates (soft vs hard errors)

**See:** [Monitoring GetBatch](/docs/monitoring-get-batch.md) for detailed metrics, PromQL queries, and operational guidance.

### CLI monitoring example

```console
$ ais show job --refresh 10

get-batch[NeEra51oM] (ctl:
  pending:(1,1) reqs:21849 objs: [1398300 2.00GiB] files: [391284 8.70GiB], bewarm:on avg-wait:2.3ms
  pending:(2,2) reqs:22188 objs: [1419972 2.03GiB] files: [395120 8.78GiB], bewarm:on avg-wait:2.2ms
  pending:(3,3) reqs:22210 objs: [1421380 2.03GiB] files: [395940 8.80GiB], bewarm:on avg-wait:2.1ms
)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1          NeEra51oM       get-batch       ais://nnn       1419972         2.03GiB         14:38:20        -       Running
target2          NeEra51oM       get-batch       ais://nnn       1421380         2.03GiB         14:38:20        -       Running
target3          NeEra51oM       get-batch       ais://nnn       1398300         2.00GiB         14:38:20        -       Running
                                Total:                          4239652         6.06GiB ✓
------------------------------------------------------------------------
get-batch[NeEra51oM] (ctl:
  pending:(1,1) reqs:25099 objs: [1606276 2.30GiB] files: [445880 9.95GiB], bewarm:on avg-wait:2.0ms
  pending:(2,2) reqs:25360 objs: [1622980 2.32GiB] files: [450110 10.05GiB], bewarm:on avg-wait:1.9ms
  pending:(2,2) reqs:25435 objs: [1627780 2.33GiB] files: [451090 10.07GiB], bewarm:on avg-wait:1.9ms
)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1          NeEra51oM       get-batch       ais://nnn       1627780         2.33GiB         14:38:20        -       Running
target2          NeEra51oM       get-batch       ais://nnn       1622980         2.32GiB         14:38:20        -       Running
target3          NeEra51oM       get-batch       ais://nnn       1606276         2.30GiB         14:38:20        -       Running
                                Total:                          4857036         6.95GiB ✓
------------------------------------------------------------------------
get-batch[NeEra51oM] (ctl:
  pending:(1,1) reqs:28729 objs: [1838584 2.63GiB] files: [512770 11.38GiB], bewarm:on avg-wait:1.8ms
  pending:(2,2) reqs:28540 objs: [1826488 2.61GiB] files: [510330 11.33GiB], bewarm:on avg-wait:1.8ms
  pending:(3,3) reqs:28343 objs: [1813892 2.59GiB] files: [507940 11.28GiB], bewarm:on avg-wait:1.7ms
)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1          NeEra51oM       get-batch       ais://nnn       1838584         2.63GiB         14:38:20        -       Running
target2          NeEra51oM       get-batch       ais://nnn       1826488         2.61GiB         14:38:20        -       Running
target3          NeEra51oM       get-batch       ais://nnn       1813892         2.59GiB         14:38:20        -       Running
                                Total:                          5478964         7.84GiB ✓
```

> CLI will render `CtlMsg` output on multiple lines when it includes multiple aggregated messages.

---

## Advanced Use Cases

### ML Training: Deterministic Epoch Loading
```python
from aistore.sdk import Client
import tarfile
from io import BytesIO

def load_epoch(epoch_num, num_shards=10000):
    """Load training data for specific epoch with deterministic ordering."""

    client = Client("http://aistore-gateway")
    bucket = client.bucket("training-data")

    # Build batch: extract same sample from 10K shards
    batch = client.batch(bucket=bucket, output_format=".tar", cont_on_err=True)
    for i in range(num_shards):
        batch.add(
            f"shard-{i:06d}.tar",
            archpath=f"samples/epoch_{epoch_num}.jpg"
        )

    # Stream TAR directly into training loop
    for moss_out, content in batch.get():
        if not moss_out.err_msg:  # Skip missing
            yield content

# Usage in training loop
for epoch in range(num_epochs):
    for sample_bytes in load_epoch(epoch):
        image = decode_image(sample_bytes)
        train_step(image)
```

### Distributed Processing: Scatter-Gather Pattern
```python
# Partition dataset across workers
def get_worker_partition(worker_id, num_workers, total_objects):
    """Each worker fetches its partition via GetBatch."""

    client = Client("http://aistore-gateway")
    bucket = client.bucket("dataset")

    # Calculate this worker's range
    objects_per_worker = total_objects // num_workers
    start = worker_id * objects_per_worker
    end = start + objects_per_worker

    # Fetch partition in one batch
    batch = client.batch(bucket=bucket)
    for i in range(start, end):
        batch.add(f"object-{i:08d}.bin")

    return batch.get()

# Worker process
for moss_out, data in get_worker_partition(worker_id=0, num_workers=10, total_objects=1_000_000):
    process(data)
```

---

## Limitations & Future Work

### Supported scope

GetBatch works on **in-cluster data**: sharded (with shards of any kind — TAR, TAR.GZ, TAR.LZ4, ZIP), or plain monolithic objects, or chunked objects. As long as the requested data is stored on the cluster's target disks, GetBatch will assemble and return it.

Per-entry **range reads** are supported for objects (chunked or monolithic) as well as for archived files: set `start`/`length` to retrieve a byte range instead of the whole object (see [Example 5](#example-5-range-reads-partial-object-retrieval)).

For TAR shards, GetBatch reads files via the [shard index](https://docs.nvidia.com/aistore/relnotes/v4.5#shard-index) fast path when an index has been built for the shard — extracting an `archpath` entry becomes direct random access into the archive instead of a linear scan. The index is persisted in the `ais://.sys-shardidx` system bucket and is consulted transparently by both `GET` and GetBatch.

### Known limitations

**Remote (cold) GET is not supported.**
When GetBatch is invoked on a remote bucket (`s3://`, `gs://`, `az://`, `oc://`) and one or more requested objects are *not already stored on any target's disks*, GetBatch will not fetch them from the backend. Instead, each missing object is recorded as a soft-error placeholder (`__404__/<objname>` in the output archive), and the request as a whole may fail when the soft-error budget (`max_soft_errs`) is exceeded. The requested objects must be pre-loaded into the cluster (e.g., via `ais bucket prefetch`, an explicit `ais get`, or a separate warmup pass) before issuing GetBatch.

**Cluster membership changes are disruptive to in-flight requests.**
Any event that mutates the cluster map (target restart, pod reschedule, scale-up, scale-down, primary proxy failover) will:

- Abort all currently-running GetBatch work items with `reason: starting x-rebalance[...]` and surface as `ErrNotFound: (prep-rx not done?)` on the client.
- Trigger an automatic global rebalance.
- May leave inter-target shared-DM streams in an unrecoverable state until the affected targets are re-bounced. Re-issuing GetBatch immediately after a membership event may fail with `context deadline exceeded` for several minutes.

Clients should retry GetBatch after a brief backoff once the rebalance completes. For long-running training pipelines, treat GetBatch failures during/after a membership event as expected and rely on the client-side retry path.

**Shard extraction is sequential within each archive.**
When multiple files are requested from the same shard, they are extracted in sequence rather than in parallel. This is a performance limitation, not a correctness issue — and is largely mitigated when the [shard index](https://docs.nvidia.com/aistore/relnotes/v4.5#shard-index) is enabled, which converts each per-file lookup from a linear scan into direct random access.

### Roadmap

- Self-healing shared-DM bundles across Smap version changes
- Multi-file parallel extraction from a single shard
- Finer-grained work item abort controls

---

## References

- [Release Notes 4.0](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - GetBatch introduction
- [Go API](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go) - API control structures
- [Python Integrations](#python-integrations)
- [Monitoring GetBatch](/docs/monitoring-get-batch.md) - Metrics and observability
- [AIS CLI](/docs/cli.md) - Command-line tools
