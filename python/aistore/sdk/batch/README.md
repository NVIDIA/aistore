# Batch Request Module for the AIStore Python SDK

The Batch Requests Module in the Python SDK provides efficient mechanisms for loading multiple objects in a single request, significantly reducing network overhead and improving data loading performance for machine learning training workloads. By leveraging AIStore's distributed architecture, batch requests can aggregate data across the cluster and stream it back as a compressed archive, optimizing throughput for high-performance training scenarios.

![Batch Request Dataflow](/docs/images/batch-dataflow.png)

## Features

- **[Batch Object Loading](#batch-object-loading)**: Load multiple objects in a single request, reducing network round-trips and connection overhead
- **[Archive Response Types](#archive-response-types)**: Support for multiple archive formats (TAR, TAR.GZ, ZIP) with optional compression
- **[Archive Path Support](#archive-path-support)**: Extract specific files from archive objects (e.g., WebDataset format) without loading entire archives
- **[Distributed Data Assembly](#distributed-data-assembly)**: Leverage AIStore's target nodes to assemble data in parallel across the cluster
- **[Streaming Response](#streaming-response)**: Stream batched data directly to clients with ordered object delivery

## Benefits

- **Reduced Network Overhead**: Eliminates multiple round-trip times by combining requests into a single batch operation
- **Improved Throughput**: Takes advantage of AIStore's distributed architecture for parallel data loading
- **Lower Memory Usage**: Avoid loading entire shards into memory when only specific samples are needed
- **Compression Support**: Optional compression reduces bandwidth usage in network-constrained environments
- **Scalable Performance**: Network overhead per sample decreases as batch size increases

## Batch Object Loading

The batch request mechanism allows you to load multiple objects with a single request, dramatically reducing the network overhead compared to individual object requests.

```python
from aistore.sdk import Client, BatchRequest

client = Client("http://localhost:8080", timeout=None)

batch_loader = client.batch_loader()

batch_req = BatchRequest(
    output_format=".tar",
    continue_on_err=True,
    only_obj_name=False,
    streaming=True,
)

obj1 = client.bucket("example").object("obj1")
obj2 = client.bucket("example").object("obj2")


batch_req.add_object_request(obj=obj1)
batch_req.add_object_request(obj=obj2)

batch = batch_loader.get_batch(batch_req)

for metadata, data in batch:
    print(metadata, len(data))
```

## Archive Response Types

Batch requests support multiple archive formats to optimize for different use cases, from fast uncompressed access to bandwidth-efficient compressed transfers.

```python
batch_req = BatchRequest(
    + output_format=".tar",
    + output_format=".tar.gz",
    + output_format=".tgz",
    + output_format=".zip",
)
```

## Archive Path Support

For datasets stored in archive formats like WebDataset, you can extract specific files without downloading entire archives.

```python
from aistore.sdk import Client, BatchRequest

client = Client("http://localhost:8080", timeout=None)

batch_loader = client.batch_loader()

batch_req = BatchRequest(
    output_format=".tar",
    continue_on_err=True,
    only_obj_name=False,
    streaming=True,
)

arch1 = client.bucket("example").object("archive1.tar")
arch2 = client.bucket("example").object("archive2.tar")


batch_req.add_object_request(obj=arch1, archpath="file1.txt")
batch_req.add_object_request(obj=arch2, archpath="file1.txt")

batch = batch_loader.get_batch(batch_req)

for metadata, data in batch:
    print(metadata, len(data))
```

## Performance Characteristics

Batch requests provide significant performance improvements, especially for smaller objects commonly used in ML training. Performance gains are most pronounced with smaller object sizes where network overhead dominates transfer time.

**Note:** This is a beta feature and is still in development.
