# Batch Request Module for the AIStore Python SDK

The Batch Module provides efficient mechanisms for loading multiple objects in a single request. By leveraging AIStore's distributed architecture, batch requests aggregate data across the cluster and stream it back as a compressed archive, reducing network overhead and improving throughput for high-performance training scenarios.

The Get-Batch API returns a single ordered archive (TAR by default) containing the requested objects and/or sharded files. From the caller's perspective, each request behaves like a regular synchronous GET, but you can read multiple batches in parallel.

![Batch Request Dataflow](/docs/images/batch-dataflow.png)

## Features

- **[Get-Batch API](#get-batch-api)**: Load multiple objects in a single request, reducing network round-trips and connection overhead
- **[Multi-Bucket Support](#multi-bucket-support)**: A single Get-Batch request can span any number of buckets, including remote cloud buckets
- **[Strict Ordering](#strict-ordering)**: Guarantees exact sequence - request A, B, C and the archive contains A, then B, then C
- **[Two Delivery Modes](#two-delivery-modes)**: Choose between streaming (immediate processing) or multipart (metadata header + payload)
- **[Archive Response Types](#archive-response-types)**: Support for multiple archive formats (TAR, TAR.GZ, ZIP) with optional compression
- **[Archive Path Support](#archive-path-support)**: Extract specific files from archive objects (e.g., WebDataset format) without loading entire archives
- **[Distributed Data Assembly](#distributed-data-assembly)**: Leverage AIStore's target nodes to assemble data in parallel across the cluster

## Benefits

- **Reduced Network Overhead**: Eliminates multiple round-trip times by combining requests into a single batch operation
- **Improved Throughput**: Takes advantage of AIStore's distributed architecture for parallel data loading
- **Lower Memory Usage**: Avoid loading entire shards into memory when only specific samples are needed
- **Compression Support**: Optional compression reduces bandwidth usage in network-constrained environments
- **Scalable Performance**: Network overhead per sample decreases as batch size increases
- **Cloud & Local Support**: Works seamlessly with objects stored locally or in remote cloud buckets (AWS S3, GCS, Azure, OCI)

## Get-Batch API

The batch request mechanism allows you to load multiple objects with a single request, dramatically reducing the network overhead compared to individual object requests.

### 1. Minimal Example: Batch Get With Object Names (`str`)

If you just have a list of object names (strings), you can batch get them easily:

```python
from aistore.sdk import Client

client = Client("http://localhost:8080")
bucket = client.bucket("example")

# Provide a list of object names as strings
object_names = ["obj1", "obj2", "dir/file3.jpg"]
batch = client.batch(object_names, bucket=bucket)

for obj_info, data in batch.get():
    print(f"Object: {obj_info.obj_name}, Size: {len(data)}")
```

### 2. With `Object` Instances

If you want more control (e.g. for archive subfiles, specifying a version, etc.), use `Object` instances:

```python
obj1 = bucket.object("obj1")
obj2 = bucket.object("obj2")

# As bucket was already part of obj, no need to pass bucket again
batch = client.batch([obj1, obj2])

for obj_info, data in batch.get():
    print(f"Object: {obj_info.obj_name}, Size: {len(data)}")
```

---

## Incremental Batch Building (Advanced Usage):

When you need to build a batch step-by-step (for dynamic, conditional, or really large lists), use `batch.add()`.

```python
batch = client.batch(bucket=bucket)

# Add objects by name (string)
batch.add("obj1")
batch.add("dir/file2.png")

# ... or add Object instances
batch.add(bucket.object("obj3"))

for obj_info, data in batch.get():
    print(f"Object: {obj_info.obj_name}, Size: {len(data)}")
```

**Parameter Notes:**

| Parameter         | Description                                                                                             |
|-------------------|--------------------------------------------------------------------------------------------------------|
| `bucket`          | Specify this unless every object you pass is an Object that already knows its bucket, or when passing strings. |
| `output_format`   | Controls response archive type (`".tar"`, `".tar.gz"`, `".zip"`, etc). Defaults to `.tar`                      |
| `cont_on_err`     | If `True`, continue on errors instead of stopping the whole batch. Defaults to `True`.                         |
| `streaming_get`   | If `True`, stream results as they are ready, not after the whole batch completes. Defaults to `True`.          |

---

**TIP:**  
- For most use-cases, pass a _list of object names (strings)_ and a `bucket`, or a _list of Object instances_ (no bucket needed).
- Use incremental build (`batch.add`) when your set of objects is not known upfront or needs custom options per-object.

### Two Delivery Modes

Get-Batch supports two delivery modes to accommodate different use cases:

1. **Streaming Mode (default)** (`streaming_get=True`): 
   - Starts sending data as the resulting payload is assembled
   - Best for immediate processing and lower latency
   - Data flows to the client as soon as each object is ready

2. **Multipart Mode**:
   - Returns two parts: a small JSON header (`MossOut`) with per-item status and sizes, followed by the archive payload
   - Best when you need metadata upfront (e.g., error handling, size information)
   - Allows inspection of what's in the batch before processing the data

```python
# Streaming mode - data flows immediately
batch = client.batch(objects, bucket=bucket, streaming_get=True)
for obj_info, data in batch.get():
    ...

# Multipart mode - get metadata first
batch = client.batch(objects, bucket=bucket, streaming_get=False)
for obj_info, data in batch.get():
    # obj_info contains object info, opaque, err_msg (if any), size, etc.
    ...
```

### Strict Ordering Guarantee

Get-Batch maintains **strict ordering**: if you request objects named A, B, C, the resulting archive will contain A, then B, then C, in that exact sequence. This is crucial for ML workloads that depend on consistent sample ordering across epochs.

### Multi-Bucket Support

A single Get-Batch request can span any number of buckets, including local buckets and remote cloud buckets (AWS S3, GCS, Azure, OCI). This is particularly useful for ML workloads that need to combine data from multiple sources:

```python
from aistore.sdk import Client

client = Client("http://localhost:8080")

# Create batch spanning multiple buckets
bucket1 = client.bucket("training-data")
bucket2 = client.bucket("validation-data")
bucket3 = client.bucket("s3-bucket", provider="s3")  # Remote bucket

batch = client.batch(bucket=bucket1)
batch.add("train_sample_1.jpg")
batch.add("train_sample_2.jpg")

# Add objects from different bucket
batch.add(bucket2.object("val_sample_1.jpg"))
batch.add(bucket3.object("cloud_sample.jpg"))

# All objects returned in a single archive, in order
for obj_info, data in batch.get():
    print(f"Bucket: {obj_info.bucket}, Object: {obj_info.obj_name}")
```



## Archive Response Types

Batch requests support multiple archive formats to optimize for different use cases, from fast uncompressed access to bandwidth-efficient compressed transfers.

```python
# Uncompressed TAR (fastest, default)
batch = client.batch(objects, bucket=bucket, output_format=".tar")

# Compressed TAR.GZ (smaller bandwidth)
batch = client.batch(objects, bucket=bucket, output_format=".tar.gz")

# TGZ format (alternative)
batch = client.batch(objects, bucket=bucket, output_format=".tgz")

# ZIP format
batch = client.batch(objects, bucket=bucket, output_format=".zip")
```

## Archive Path Support

For datasets stored in archive formats like WebDataset, you can extract specific files without downloading entire archives.

```python
from aistore.sdk import Client

client = Client("http://localhost:8080", timeout=None)
bucket = client.bucket("example")

# Get archive objects
arch1 = bucket.object("archive1.tar")
arch2 = bucket.object("archive2.tar")

# Create batch and extract specific files from archives
batch = client.batch(bucket=bucket)
batch.add(arch1, archpath="file1.txt")
batch.add(arch2, archpath="file1.txt")

# Execute and get extracted files
for obj_info, data in batch.get():
    print(f"Archive: {obj_info.obj_name}, File: {obj_info.archpath}, Size: {len(data)}")
```

### Advanced Features

You can also add opaque tracking data to batch requests:

```python
# Add opaque data for tracking/correlation
batch = client.batch(bucket=bucket)
batch.add("file1.txt", opaque=b"user-id-123")
batch.add("archive.tar", archpath="data.json", opaque=b"request-456")

for obj_info, data in batch.get():
    # Opaque data is returned in obj_info
    print(f"Object: {obj_info.obj_name}, Tracking: {obj_info.opaque}")
```

## Performance Characteristics

Get-Batch provides the largest gains for small-to-medium object sizes, where it effectively amortizes TCP and connection-setup overheads across multiple requests. For larger objects, overall performance improvement tapers off because the data transfer time dominates total latency, making the per-request network overhead negligible in comparison.

## Additional Resources

- [Blog Post: Get-Batch API](https://aistore.nvidia.com/blog/2025/10/06/get-batch-sequential) - Detailed performance analysis and use cases
- [AIStore 4.0 Release Notes](https://github.com/NVIDIA/aistore/releases/tag/v1.4.0) - Full release information
- [Python SDK Documentation](/docs/python_sdk.md) - Complete SDK reference
- [Example Notebook](https://github.com/NVIDIA/aistore/tree/main/python/examples/sdk/ais-batch-requests.ipynb) - Jupyter notebook with examples
