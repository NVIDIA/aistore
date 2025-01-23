---
layout: post
title: "Adding Data to AIStore -- PUT Performance"
date: November 22, 2024
author: Aaron Wilson
categories: aistore training put promote benchmarks
--- 

AI training workloads primarily _read_ data, and lots of it.
So naturally, for most of our [previous blogs](https://aistore.nvidia.com/blog/2023/11/27/aistore-fast-tier) and benchmarks we've focused heavily on GET performance.
However, for any storage solution, the ability to add and modify data - and do it reliably and efficiently - is also critically important.
In this article we'll look at some performance benchmarks and options for getting data into AIStore. 

## Benchmark Setup

First, we ran a series of benchmarks to determine AIS performance given a simple PUT workload. 

Our AIStore cluster is a production-level 16-node cluster running in OCI (Oracle Cloud Infrastructure) managed Kubernetes.
We've selected the [E5 DenseIO nodes](https://docs.oracle.com/iaas/Content/Compute/References/computeshapes.htm#bm-dense), each with 12 5.8 TiB NVME drives and 100 Gb networking. 

For the benchmark client we used a single instance of the same node shape (E5 DenseIO) running our [AISLoader client](https://github.com/NVIDIA/aistore/blob/main/docs/aisloader.md) -- a simple multi-threaded HTTP-based workload generator. 
For this test we ran the client with 10 worker threads in order to keep the test small and avoid any external factors. 
With larger numbers of client threads, we could run into rate limits imposed by AWS S3 (3,500 PUTs per prefix at time of writing) or throughput limits of our outbound network. 

We ran 3 tests: 
1. Direct requests to our AWS S3 bucket
2. Requests to the AIS bucket with a remote AWS S3 backend bucket
3. Direct requests to a local AIS bucket with no backend

For each bucket, we utilized the default AIS configuration with no mirroring and no erasure coding. 

## Benchmark Results

![10 MiB Bench Run](/assets/put_performance/10MiB_comparison.png)
![10 MiB PUT Throughput](/assets/put_performance/10MiB_throughput.svg)

Above are the results for the PUT benchmark of 20GiB total of 10 MiB objects. 
As expected, there is a measurable performance drop from direct PUT due to the overhead of writing the objects to the AIS disks before uploading them to the backend bucket. 
We see around 66% of the throughput when compared to direct S3 writes. 
Writing directly to AIS, however, is far faster when S3 is not involved. 

We also ran a few benchmarks for 100 GiB total of 100 MiB objects:

![100 MiB Bench Run](/assets/put_performance/100MiB_comparison.png)
![100 MiB PUT Throughput](/assets/put_performance/100MiB_throughput.svg)

Here we see significantly less impact from the local write, as most of the time for each object is simply waiting for the S3 upload. 
AIS shows less overhead as the size of PUT increases. 
And again, writing directly to AIS is an order of magnitude faster. 

## Conclusion

These benchmark numbers show the performance penalty of the extra write to AIS before backend upload in a couple of very specific scenarios.
The numbers themselves may vary depending on machines, workloads, etc., but they give us some general expectations.
As object size increases, the local write causes significantly less overhead.
Asynchronous write-back is one potential (unimplemented) feature that could alleviate this penalty -- writing to disk at the same time as remote. 
This feature would bring its own tradeoffs and challenges with consistency, but potentially improve overall PUT performance.
A currently supported feature in AIS, ["promote"](https://aistore.nvidia.com/blog/2022/03/17/promote), provides a more direct and performant way to load locally available data into AIS. 
In future benchmarks we plan to include comparisons with `promote` and other approaches to increase data ingestion throughput. 

## References

- [AIStore](https://github.com/NVIDIA/aistore)
- [AIS-K8S](https://github.com/NVIDIA/ais-K8s)
- [AISLoader](https://github.com/NVIDIA/aistore/blob/main/docs/aisloader.md)
- [AIS as a Fast-Tier](https://aiatscale.org/blog/2023/11/27/aistore-fast-tier)
- [AIS Promote](https://aistore.nvidia.com/blog/2022/03/17/promote)