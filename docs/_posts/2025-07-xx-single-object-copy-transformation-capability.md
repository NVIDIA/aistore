---
layout: post
title: "Single-Object Copy/Transform Capability"
date: July xx, 2025
author: Tony Chen
categories: aistore cli etl benchmark optimization enhancements
---

# Single-Object Copy/Transform Capability

In version 3.30, AIStore introduced a lightweight, flexible API to copy or transform a single object between buckets. It provides a simpler alternative to existing batch-style operations, ideal for fast, one-off object copy or transformation without the overhead of a full-scale job.

Notably, both the source and destination can be the local AIStore cluster or any of its [remote backends](https://github.com/NVIDIA/aistore/blob/main/docs/images/supported-backends.png) (e.g., `s3://src/a` => `gs://dest/b`), making this feature especially useful for ad-hoc workflows and lightweight data preparation.

In this post, we'll walk through the design and internal workflow that make this capability possible. We'll also demonstrate how to use it with various supported clients, and compare it with existing copy mechanisms in AIStore to help you choose the right one for your use case.

## Features Highlight

AIStore supports a variety of copy-object features, including [bucket copy](/docs/cli/bucket.md#copy-cloud-bucket-to-another-cloud-bucket) and [multi-object copy](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets). However, these operations are designed as batch jobs that involve a more complex setup across the cluster to ensure all storage targets are ready and connected. While cluster-wide coordination ensures job can be executed or aborted seamlessly, it introduces noticeable overhead upfront — an unnecessary cost when the operation doesn't require participation from all storage targets.

In contrast, the newly introduced single-object copy operation takes a simpler and more lightweight approach. It directly transfers the object from the source to the destination target in a single, synchronous step - bypassing the need for cluster-wide coordination and setup.

This direct transmission approach also allows the single-object copy operation to bypass the client entirely, offloading the responsibility of handling the object exchange. Unlike a GET-and-PUT sequence for copying, the client never needs to fetch or upload the object. The data moves entirely within the cluster, directly from the source to the destination target - all the client does is send the command. This becomes especially beneficial as object size increases, reducing client-side overhead and network usage.

![Copy Object Diagram](/assets/copy_object_diagram.png)

Additionally, the single-object copy workflow integrates seamlessly with ETL transformations. When an ETL is specified in the request's parameter, the source target streams the object bytes to a local ETL container for transformation. Once processed, the transformed bytes are forwarded directly to the destination target — again, without routing through the client.

> For more details on the direct put optimization, please refer to [this documentation](/docs/etl.md#direct-put-optimization).

## Usage

### AIStore CLI

Here’s a quick example of how to use the single-object copy feature with the CLI:

```
# Upload a local file to the source bucket
$ ais object put README.md ais://src/aaa
PUT "README.md" => ais://src/aaa

# Copy the object from AIStore to a GCP bucket
$ ais object cp ais://src/aaa gs://dest/bbb
COPY ais://src/aaa => gs://dest/bbb

# Download and verify the copied object
$ ais object get gs://dest/bbb
GET bbb from gs://dest as bbb (11.24KiB)
```

Using the feature with ETL transformation is just as straightforward. It follows the standard `ais etl` command pattern: specify the subcommand (`object`), provide the ETL name, and pass in the arguments.
Here’s an example that computes an object’s MD5 hash via single-object transformation:

```
# Initialize an ETL transformer to compute MD5 hash values
$ ais etl init --name md5-etl -f https://raw.githubusercontent.com/NVIDIA/ais-etl/main/transformers/md5/etl_spec.yaml

# Perform a single-object transformation
$ ais etl object md5-etl cp ais://src/aaa ais://dest/bbb
ETL[md5-etl]: ais://src/aaa => ais://dest/bbb

# Retrieve the transformed object (MD5 hash value)
$ ais object get ais://dest/bbb -
# MD5 hash value of the original object "ais://src/aaa"
```

### AIStore Python SDK

The [Python SDK](/docs/python_sdk.md) provides an intuitive interface for using the single-object copy API.

```python
# Create source and destination buckets
src_bck = client.bucket("src").create(exist_ok=True)
dest_bck = client.bucket("dest").create(exist_ok=True)

# Upload an object to the source bucket
src_obj = src_bck.object("aaa")
src_obj.get_writer().put_content(b"Hello World!")

# Prepare a destination object handle, and perform the copy operation
dest_obj = dest_bck.object("bbb")
src_obj.copy(dest_obj)

# Verify that the object was copied correctly
print(dest_obj.get_reader().read_all())
# Output: b'Hello World!'
```

To apply an ETL transformation as part of the copy operation, simply pass an `ETLConfig` to the `copy()` method. The SDK automatically handles the required parameter population:

```python
# Define and initialize a simple ETL that reverses object content
etl_reverse = client.etl("etl-reverse")

@etl_reverse.init_class()
class UpperCaseETL(FastAPIServer):
    def transform(self, data, *_args):
        return data[::-1]

# Perform a copy with ETL transformation applied
from aistore.sdk.etl import ETLConfig
src_obj.copy(to_obj=dest_obj, etl=ETLConfig(etl_reverse.name))

# Confirm the transformation result
print(dest_obj.get_reader().read_all())
# Output: b'!dlroW olleH'
```

### S3 Client

The single-object copy feature is also accessible via any S3-compatible client. For example, using [`s3cmd`](https://s3tools.org/s3cmd), you can copy objects between buckets without any changes to your existing S3-based workflows.

First, install `s3cmd` and configure it to connect to your AIStore cluster by following the [S3 client configuration guide](/docs/s3compat.md#configuring-clients).

Once configured, here's how you can perform a simple object copy:

```
# Confirm the source object exists
$ ais ls ais://src
NAME             SIZE            
README.md        11.24KiB        

# Confirm the destination is initially empty
$ ais ls ais://dest
No objects in ais://dest

# Use s3cmd to copy the object from src to dest
$ s3cmd cp s3://src/README.md s3://dest
remote copy: 's3://src/README.md' -> 's3://dest/README.md'  [1 of 1]

# Verify the copied object is accessible in the destination bucket
$ ais object get ais://dest/README.md
GET README.md from ais://dest as README.md (11.24KiB)
```

## Performance Comparison

To better understand when to use the single-object copy API versus the job-based copy bucket mechanism, we ran a set of performance benchmarks across varying object sizes and workloads.

This scenario focuses on copying just one object at a time. We evaluated the three supported approaches across different object sizes.

- **Client-Side Copy**: The simplest method. It retrieves the object with a GET, then re-upload it to the destination using PUT. The client handles the full object payload.
- **Single-Object Copy API**: Performs a direct, in-cluster transfer from source to destination, bypassing the client entirely.
- **Job-Type Copy Bucket API**: Launches a cluster-wide job to move the object, even when there's only one object involved.

![Copy Performance Comparison](/assets/copy_performance.png)

| Method                       | 64 KB  |  1 MB  | 16 MB  | 256 MB |  1 GB   |  4 GB   |
|------------------------------|--------|--------|--------|--------|---------|---------|
| Client-Side Copy             | 0.007s | 0.021s | 0.670s | 2.515s | 16.283s | 56.407s |
| Single-Object Copy API       | 0.004s | 0.006s | 0.027s | 0.338s | 1.172s  | 5.0060s |
| Job-Type API (Copy Bucket)   | 13.08s | 13.07s | 13.08s | 13.08s | 14.123s | 19.147s |

As expected, the single-object copy API significantly outperforms the client-side method, especially as object size increases. Involving the client introduces unnecessary latency — effectively pulling data out of and back into the cluster. The job-type API introduces coordination overhead that isn't justified for single-object transfers.

> **Note:** The relative performance order remains consistent even when an ETL transformation is applied during the copy. In each case, the transformation just adds one extra network step between the target and its ETL container. We ran the same tests with ETL included and confirmed that performance ranking across the three approaches did not change.

## Conclusion

The single-object copy API is a fast, low-overhead solution tailored for one-off object transfers. Whether you’re moving data between internal buckets or bridging between cloud backends, it delivers consistent performance without the setup cost of a full job. It's the ideal choice for lightweight workflows and ad-hoc object manipulation where efficiency matters.

## References

- [AIS Repository](https://github.com/NVIDIA/aistore)
- [AIStore ETL Overview](/docs/etl.md)
- [AIS Python SDK PyPI](https://pypi.org/project/aistore/)
- [AIS CLI Documentation](/docs/cli.md)
