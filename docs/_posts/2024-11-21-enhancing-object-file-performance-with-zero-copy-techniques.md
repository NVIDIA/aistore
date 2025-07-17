---
layout: post
title: "Enhancing ObjectFile Performance with Zero-Copy Techniques"
date: November 21, 2024
author: Ryan Koo, Aaron Wilson
categories: aistore python resilient streaming benchmark optimization enhancements
---

# Enhancing ObjectFile Performance with Zero-Copy Techniques

In our [previous blog post](https://aistore.nvidia.com/blog/2024/09/26/resilient-streaming-with-object-file), we introduced `ObjectFile`, a resilient, file-like interface in the AIStore Python SDK designed to handle large, streamed datasets efficiently. `ObjectFile` addresses the challenges of network interruptions and unexpected node failures by automatically handling unexpected stream interruptions (e.g. `ChunkedEncodingError`,`ConnectionError`) or timeouts (e.g. `ReadTimeout`). This allows streams to resume seamlessly from the last successful byte, ensuring uninterrupted data access even in unstable network conditions — a critical feature for long-running tasks like training models on massive datasets.

While `ObjectFile` provided significant improvements in reliability by handling intermittent failures gracefully, we identified opportunities to optimize its performance. In this post, we'll delve into recent enhancements that leverage zero-copy techniques using `memoryview`, resulting in substantial performance gains when handling large-scale data.


## The Problem: Extra Memory Copies

In the initial implementation, ObjectFile fetched data chunks as `bytes` and used a `bytearray` to accumulate the chunks before returning the requested data. This introduced an unnecessary intermediary copy in memory:

1. **Appending to Bytearray**: Each chunk fetched as `bytes` was appended to a `bytearray` buffer, which involved a memory copy of the entire data chunk.
2. **Converting to Bytes**: When returning the requested data, the buffer was cast back to `bytes`, creating yet another memory copy.

This two-step process resulted in an extra memory copy for every read operation, which increased overhead and reduced performance, particularly when handling large chunks of data or high-throughput streams.


## The Solution: Using a List of MemoryViews

To eliminate the extra copy, the new implementation accumulates data in a list of `memoryview` objects instead of a `bytearray`. [memoryview](https://docs.python.org/3/c-api/memoryview.html) is a Python object that exposes the buffer interface of our `bytes` objects, so we can slice and store references to that data without extra copies. Here’s how it works:

- Each fetched chunk is converted to a `memoryview`, which creates a lightweight reference to the data without copying it.
- These `memoryview` objects are appended directly to a list.
- At the end of the read operation, the `memoryview` objects are concatenated and cast to a single `bytes` object in one final step.

By removing the intermediary `bytearray`, this approach avoids the unnecessary copy when appending data. Now, only a single copy is made during the final cast to `bytes`.


## Benchmarking ObjectFile Performance

To assess the performance improvements from these optimizations, we conducted benchmarks comparing the enhanced `ObjectFile` with the baseline `ObjectReader.raw()` method, which returns a `requests.response.raw` stream. The goal was to measure the throughput of data extraction when reading from both types of file-like objects.

We used `tarfile.extractall()` to extract the contents of a tarball stored in AIStore, measuring the time taken to complete the extraction using each method.

Each benchmark was run over 500 iterations to ensure reliable results and account for variability. The benchmarks were conducted from a separate client machine located within the same network as the AIS cluster, simulating a realistic environment with low network overhead. We measured throughput in megabytes per second (MB/s), calculating the median value to reduce the impact of outliers or transient network fluctuations.

> For more details on the benchmarking setup and to view the source code, please refer [here](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk/object_file/obj-read-benchmark.py).


| Total Size | File Size | File Count | ObjectReader Throughput (MB/s) | ObjectFile Throughput (MB/s) | Improvement (%) |
|------------|-----------|------------|--------------------------------|------------------------------|-----------------|
| 500 MB | 1 MB | 500 | 370.98 | 425.57 | 14.72 |
| 500 MB | 10 MB | 50 | 380.22 | 453.64 | 19.31 |
| 500 MB | 100 MB | 5 | 400.43 | 469.54 | 17.25 |
| 1 GB | 1 MB | 1000 | 372.75 | 424.17 | 13.79 |
| 1 GB | 10 MB | 100 | 391.33 | 453.39 | 15.86 |
| 1 GB | 100 MB | 10 | 398.15 | 466.54 | 17.18 |
| 10 GB | 1 MB | 10000 | 377.84 | 434.25 | 14.93 |
| 10 GB | 10 MB | 1000 | 405.66 | 463.46 | 14.25 |
| 10 GB | 100 MB | 100 | 406.38 | 468.95 | 15.40 |

The benchmark results demonstrate that `ObjectFile` consistently achieves higher throughput compared to the baseline `ObjectReader.raw()` method across the various configurations tested. Improvements in throughput ranged from approximately **13% to 19%** and were observed across different total sizes, file sizes, and file counts. These gains were evident whether dealing with smaller tarballs of 500 MB or larger ones up to 10 GB, and regardless of the individual file sizes within the tarballs.

This performance improvement is primarily due to `ObjectFile` avoiding an extra memory copy per chunk by aggregating data directly. While `tarfile` could potentially adopt similar optimizations internally, `ObjectFile` provides efficient handling of streamed data for _any_ caller requiring a file-like object.

## Conclusion

`ObjectFile` provides a memory-optimized, performant, and reliable solution for handling large, streamed datasets. It delivers significant throughput improvements over the existing raw object reader while offering the ability to resume interrupted streams and providing a familiar file-like interface. These enhancements make `ObjectFile` an efficient and performant alternative for large-scale data processing, especially in environments where efficiency and speed are critical.

Looking ahead, we aim to further enhance `ObjectFile` with parallelized read-ahead functionality, enabling even faster data access by overlapping data fetching with processing.


## References

- [AIS Repository](https://github.com/NVIDIA/aistore)
- [AIS Python SDK PyPI](https://pypi.org/project/aistore/)
- ObjectFile
    - [Source](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/obj/obj_file/object_file.py)
    - [Docs](https://github.com/NVIDIA/aistore/blob/main/docs/python_sdk.md#obj.obj_file.object_file.ObjectFile)
    - [Demo](https://github.com/NVIDIA/aistore/blob/main/python/examples/sdk/resilient-streaming-object-file.ipynb)
- [Benchmark Script](https://github.com/NVIDIA/aistore/blob/main/python/tests/perf/object_file/obj-read-benchmark.py)
- [Python MemoryView Documentation](https://docs.python.org/3/c-api/memoryview.html)
- [Python `io.BufferedIOBase` Documentation](https://docs.python.org/3/library/io.html#io.BufferedIOBase)