# GetBatch: Multi-Object Retrieval API

## Overview

GetBatch is AIStore's high-performance API for retrieving multiple objects and/or archived files in a single request. It assembles the requested data into a TAR (or compressed) archive and delivers it as a continuous stream.

> GetBatch is implemented by the extended action (job) codenamed "x-moss" and is available via `/v1/ml/moss` API endpoint.

Regardless of retrieval source (in-cluster objects, remote objects, or shard extractions), GetBatch always preserves the **exact ordering** of request entries in both streaming and buffered modes.

Other supported capabilities include:

- Fetch thousands of objects in strict user-specified order
- Extract specific files from distributed shard archives (TAR/ZIP/TGZ/LZ4)
- Cross-bucket retrieval in a single request
- Graceful handling of missing data
- Streaming or buffered delivery

> Note: buffered mode always returns both metadata (that describes the output) and the resulting serialized archive containing all requested data items.

> Note: for TGZ, both .tgz and .tar.gz are accepted (and interchangeable) aliases - both denote the same exact format.

**A Note on HTTP Semantics**

GetBatch uses **HTTP GET with a JSON request body**, which is:
- Permitted by [RFC 9110](https://www.rfc-editor.org/rfc/rfc9110.html) - HTTP semantics allow message bodies in GET requests.
- Necessary for this API, as the list of requested objects can contain thousands of entries that would exceed URL length limits.
- Semantically correct - the operation is idempotent (pure data retrieval with no server-side state changes).

Rest of this document is structured as follows:

**Table of Contents**
* [Overview](#overview)
* [Supported APIs](#supported-apis)
* [Terminology](#terminology)
* [When to Use GetBatch](#when-to-use-getbatch)
* [When NOT to Use GetBatch](#when-not-to-use-getbatch)
* [Go API Structure](#go-api-structure)
* [Python Integrations](#python-integrations)
* [Usage Examples](#usage-examples)
* [Performance Characteristics](#performance-characteristics)
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

The respective Go and Python-based  usage examples follow below, in the sections that include:

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

- **Single objects** => Use regular GET
- **Small batches (<16 entries)** => Overhead not justified; use parallel GETs
- **Order doesn't matter** => Use list-objects + concurrent GETs for better parallelism
- **Real-time random access** => GetBatch optimizes for sequential streaming

> Whether or not batch sizes can be deemed "small" will ultimately (obviously) depend on the workload and cluster; any specific numbers in this document must be considered a "rule of thumb" rather than direct prescription or limitation of any kind.

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
| `start` | int64 | Range read start offset (future) |
| `length` | int64 | Range read length (future) |

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

GetBatch is not limited to the Go API - it is also integrated with Python data loading stacks in two ways:

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
A complete, runnable example for batch loading audios from AIStore with Lhotse is available in [here](https://github.com/lhotse-speech/lhotse/blob/master/examples/06-train-ais-batch.ipynb):

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

### 3. PyTorch Integration

The [AIStore PyTorch Plugin](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch) provides `AISBatchIterDataset`, an iterable-style dataset that uses GetBatch API for efficient multi-worker data loading with automatic batching and streaming support.

---

## Usage Examples

Note: `curl` examples in this section are **purely illustrative** - do not copy/paste.

### Example 1: Retrieve Plain Objects

```console
curl -L -X GET http://aistore-gateway/v1/ml/moss/my-bucket \
  -H "Content-Type: application/json" \
  -d '{
    "action": "getbatch",
    "value": {
      "mime": ".tar",
      "in": [
        {"objname": "file-0001.bin"},
        {"objname": "file-0002.bin"},
        {"objname": "file-0003.bin"}
      ],
      "strm": true
    }
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
# Extract image_42.jpg from 100 distributed shards
curl -L -X GET http://aistore-gateway/v1/ml/moss/shards \
  -H "Content-Type: application/json" \
  -d '{
    "action": "getbatch",
    "value": {
      "mime": ".tar",
      "in": [
        {"objname": "shard-0000.tar", "archpath": "image_42.jpg"},
        {"objname": "shard-0001.tar", "archpath": "image_42.jpg"},
        ...
        {"objname": "shard-0099.tar", "archpath": "image_42.jpg"}
      ],
      "onob": true,
      "strm": true
    }
  }' --output extracted.tar
```

Result: `extracted.tar` containing:
```
shard-0000.tar/image_42.jpg
shard-0001.tar/image_42.jpg
...
shard-0099.tar/image_42.jpg
```

### Example 3: Cross-Bucket Retrieval

> The request bucket (`default-bucket` in URL) is used when bucket is omitted in an in entry.

```console
curl -L -X GET http://aistore-gateway/v1/ml/moss/default-bucket \
  -H "Content-Type: application/json" \
  -d '{
    "action": "getbatch",
    "value": {
      "in": [
        {"objname": "config.json", "bucket": "configs"},
        {"objname": "model.pt", "bucket": "models"},
        {"objname": "data.csv", "bucket": "datasets"}
      ],
      "strm": true
    }
  }' --output multi-bucket.tar
```

Result: TAR containing objects from three different buckets in one request.

### Example 4: Handle Missing Data Gracefully

```console
curl -L -X GET http://aistore-gateway/v1/ml/moss \
  -H "Content-Type: application/json" \
  -d '{
    "action": "getbatch",
    "value": {
      "in": [
        {"objname": "exists-1.bin"},
        {"objname": "missing.bin"},
        {"objname": "exists-2.bin"}
      ],
      "coer": true,
      "strm": false
    }
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

## Performance Characteristics

### Throughput by Workload Type

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

get-batch[NeEra51oM] (run options: objs:1398300 size:2.00GiB, reqs:21849, bewarm:on; pending:1 objs:1419972 size:2.03GiB, reqs:22188, bewarm:on; pending:2 objs:1421380 size:2.03GiB, reqs:22210, bewarm:on)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1         NeEra51oM       get-batch       ais://nnn       1419972         2.03GiB         14:38:20        -       Running
target2         NeEra51oM       get-batch       ais://nnn       1421380         2.03GiB         14:38:20        -       Running
target3         NeEra51oM       get-batch       ais://nnn       1398300         2.00GiB         14:38:20        -       Running
                                Total:                          4239652         6.06GiB ✓
------------------------------------------------------------------------
get-batch[NeEra51oM] (run options: objs:1606276 size:2.30GiB, reqs:25099, bewarm:on; pending:1 objs:1627780 size:2.33GiB, reqs:25435, bewarm:on; objs:1622980 size:2.32GiB, reqs:25360, bewarm:on)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1         NeEra51oM       get-batch       ais://nnn       1627780         2.33GiB         14:38:20        -       Running
target2         NeEra51oM       get-batch       ais://nnn       1622980         2.32GiB         14:38:20        -       Running
target3         NeEra51oM       get-batch       ais://nnn       1606276         2.30GiB         14:38:20        -       Running
                                Total:                          4857036         6.95GiB ✓
------------------------------------------------------------------------
get-batch[NeEra51oM] (run options: objs:1838584 size:2.63GiB, reqs:28729, bewarm:on; pending:2 objs:1826488 size:2.61GiB, reqs:28540, bewarm:on; pending:3 objs:1813892 size:2.59GiB, reqs:28343, bewarm:on)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1         NeEra51oM       get-batch       ais://nnn       1838584         2.63GiB         14:38:20        -       Running
target2         NeEra51oM       get-batch       ais://nnn       1826488         2.61GiB         14:38:20        -       Running
target3         NeEra51oM       get-batch       ais://nnn       1813892         2.59GiB         14:38:20        -       Running
                                Total:                          5478964         7.84GiB ✓
------------------------------------------------------------------------
get-batch[NeEra51oM] (run options: pending:1 objs:2045932 size:2.93GiB, reqs:31969, bewarm:on; objs:2030828 size:2.90GiB, reqs:31733, bewarm:on; objs:2021444 size:2.89GiB, reqs:31586, bewarm:on)
NODE             ID              KIND            BUCKET          OBJECTS         BYTES           START           END     STATE
target1         NeEra51oM       get-batch       ais://nnn       2045932         2.93GiB         14:38:20        -       Running
target2         NeEra51oM       get-batch       ais://nnn       2030828         2.90GiB         14:38:20        -       Running
target3         NeEra51oM       get-batch       ais://nnn       2021444         2.89GiB         14:38:20        -       Running
                                Total:                          6098204         8.72GiB ✓
```

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

**Current limitations:**
- Range reads (`start`/`length`) not yet implemented
- Shard extraction is sequential within each archive

**Roadmap:**
- Range read support for partial object retrieval
- Multi-file extraction from single shard (performance optimization)
- Finer-grained work item abort controls

---

## References

- [Release Notes 4.0](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - GetBatch introduction
- [Go API](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go) - API control structures
- [Python Integrations](#python-integrations)
- [Monitoring GetBatch](/docs/monitoring-get-batch.md) - Metrics and observability
- [AIS CLI](/docs/cli.md) - Command-line tools
