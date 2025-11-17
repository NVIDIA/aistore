# GetBatch: Multi-Object Retrieval API

## Overview

GetBatch is AIStore's high-performance API for retrieving multiple objects and/or archived files in a single request. It assembles the requested data into a TAR (or compressed) archive and delivers it as a continuous stream.

> GetBatch is implemented by the extended action (job) codenamed "x-moss" and is available via `/v1/ml/moss` API endpoint.

**Key capabilities:**

- Fetch thousands of objects in strict user-specified order
- Extract specific files from distributed shard archives (TAR/ZIP/TGZ/LZ4)
- Cross-bucket retrieval in a single request
- Graceful handling of missing data
- Streaming or buffered delivery

> For TGZ, both .tgz and .tar.gz are accepted aliases - both denote the same exact format.

## Terminology

| Term | Description |
|------|-------------|
| **DT (Designated Target)** | Randomly selected AIS target that coordinates and assembles the get-batch response |
| **Work Item (WI)** | A single get-batch request being processed; implementation-wise, each request becomes one work item |
| **Shard** | An archive file (TAR/ZIP/TGZ/LZ4) stored in (or accessible by) the cluster and containing multiple files, typically used for dataset distribution |
| **Soft Error** | Recoverable error (missing object, transient network issue) that doesn't fail the entire request when `coer: true` |
| **Hard Error** | Unrecoverable failure that terminates the work item (e.g., 429 rejection, fatal errors) |
| **GFN (Get-From-Neighbor)** | Fallback mechanism to retrieve data from peer targets when primary location fails |
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

## API Structure

### Request: `apc.MossReq`

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
| `in`   | []MossIn | List of objects/files to retrieve (order preserved) |
| `coer` | bool | Continue on error: `true` = include missing items under `__404__/`, `false` = fail on first missing |
| `onob` | bool | Output naming: `false` = `bucket/object`, `true` = `object` only |
| `strm` | bool | Streaming mode: `true` = stream as data arrives, `false` = buffer then send multipart response |

### Request Entry: `apc.MossIn`

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

### Response: `apc.MossResp`

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

## Usage Examples

Examples below are pseudo-code provided to quickly illustrate usage (rather than copy/paste as is).

### Example 1: Retrieve Plain Objects

```bash
curl -X POST http://aistore-gateway/v1/buckets/my-bucket \
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

> pseudo-code to illustrate usage (rather than copy/paste as is)

```bash
# Extract image_42.jpg from 100 distributed shards
curl -X POST http://aistore-gateway/v1/buckets/shards \
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

```bash
curl -X POST http://aistore-gateway/v1/buckets/default-bucket \
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

```bash
curl -X POST http://aistore-gateway/v1/buckets/training \
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

```bash
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

```bash
ais config cluster get_batch.warmup_workers=4
```

To disable warmup/look-ahead operation:

```bash
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

**CLI monitoring:**
```bash
# Show active get-batch jobs
ais show job xaction get-batch

# Example output:
NODE         ID              OBJECTS    BYTES      STATE
target1      xact-123        450K       127 GiB    Running
target2      xact-123        445K       125 GiB    Running
```

---

## Advanced Use Cases

### ML Training: Deterministic Epoch Loading

```python
import requests
import tarfile

def load_epoch(epoch_num, num_shards=10000):
    """Load training data for specific epoch with deterministic ordering."""

    req = {
        "action": "getbatch",
        "value": {
            "mime": ".tar",
            "in": [
                {"objname": f"shard-{i:06d}.tar", "archpath": f"sample_{epoch_num}.jpg"}
                for i in range(num_shards)
            ],
            "strm": True,
            "coer": True  # Some shards may be missing
        }
    }

    response = requests.post(
        "http://aistore/v1/buckets/training-data",
        json=req,
        stream=True
    )

    # Stream TAR directly into DataLoader
    with tarfile.open(fileobj=response.raw, mode='r|') as tar:
        for member in tar:
            if not member.name.startswith("__404__/"):
                yield tar.extractfile(member).read()
```

### Distributed Processing: Scatter-Gather Pattern

```bash
# Scatter: Split 1M objects across 10 workers
# Gather: Each worker fetches its partition via GetBatch

# Worker 1: Objects 0-99999
curl -X POST http://aistore/v1/buckets/dataset \
  -d '{"action":"getbatch", "value":{"in":[...]}}'

# Worker 2: Objects 100000-199999
# ... and so on
```

---

## Limitations & Future Work

**Current limitations:**
- Range reads (`start`/`length`) not yet implemented
- Maximum work item size governed by memory + `max_soft_errs`
- Shard extraction is sequential within each archive

**Roadmap:**
- Range read support for partial object retrieval
- Multi-file extraction from single shard (performance optimization)
- Finer-grained work item abort controls

---

## See Also

- [Release Notes 4.0](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - GetBatch introduction
- [API Reference](https://github.com/NVIDIA/aistore/blob/main/api/apc/ml.go) - Complete API structure
- [Monitoring GetBatch](/docs/monitoring-get-batch.md) - Metrics and observability
- [AIS CLI](/docs/cli.md) - Command-line tools
- [Release Notes v1.4.0](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - GetBatch introduction
