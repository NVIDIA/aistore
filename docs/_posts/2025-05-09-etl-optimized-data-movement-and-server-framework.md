---
layout: post
title:  "AIStore v3.28: Boost ETL Performance with Optimized Data Movement and Specialized Web Server Framework"
date:   May 09, 2025
author: Tony Chen, Abhishek Gaikwad
categories: aistore etl python-sdk ffmpeg
---

The current state of the art involves executing data pre-processing, augmentation, and a wide variety of custom ETL workflows on individual client machines. This approach lacks scalability and often leads to significant performance degradation due to unnecessary data movement. While many open-source and cloud-native ETL tools exist, they typically do not account for data locality.

This is precisely why we developed an ETL subsystem for AIStore. As a distributed storage system, AIStore has precise knowledge of where every object resides within your dataset. This awareness eliminates redundant data movement, streamlining the entire pipeline and delivering, as we demonstrate below, a 35x speedup compared to conventional client-side ETL workflows.

This post highlights recent performance improvements across all three ETL stages in AIStore:

* **Extract**: Introduced **WebSocket streaming** communication mechanism between AIStore and the ETL web server.
* **Transform**: Provided a high-performance SDK framework to build ETL web server with custom transformation logic.
* **Load**: Enabled **direct put**, allowing ETL output to bypass intermediaries and write directly to the target node.

## WebSocket Streaming: Persistent and Reusable Communication Channel

In distributed systems, the choice of communication protocol can significantly impact performance. In AIStore ETL, the existing mechanisms (`Hpush`, `Hpull`, and `I/O`) rely on RESTful HTTP interfaces to transfer object bytes using request and response payloads. While this approach provides clean isolation between the TCP sessions used for each individual object, it also introduces overhead: each object transformation requires to establish a new HTTP session, which can be costly, especially for small objects.

To reduce this overhead, AIStore ETL now supports a new communication type: **WebSocket**.

WebSocket enables long-lived TCP connections that facilitate continuous data streaming between targets and ETL containers without repeatedly setting up and tearing down. In contrast, HTTP-based traffic involves repeated connection establishment and termination with an overhead that grows inversely proportionally to the transfer sizes"

Specifically, for each offline (bucket-to-bucket) transform request, AIStore:
1. Pre-establishes multiple long-lived WebSocket connections to the ETL server.
2. Distributes source objects across these connections.
3. Streams transformed object bytes back through the same connection.

![ETL WebSocket](/assets/ais_etl_series/etl-websocket-communication.png)

This upgrade significantly reduces overhead and improves throughput, especially in high-volume transformations involving small-sized objects.

## High-Performance ETL Web Server Framework

**AIStore ETL** is language- and framework-agnostic. You can deploy your own custom web server as a transformation pod, supporting both **inline transformations** (real-time GET requests) and **offline batch transformations** (bucket-to-bucket). 

However, building a custom transformation web server from scratch can be complex. Beyond just transforming data, your server must also handle:
- Health checks
- Communication with AIStore targets
- Parsing `etl_args` from requests
- Supporting `direct_put`
- Managing HTTP/WebSocket protocols and concurrency

Choosing the right framework and communication method often depends on the size and number of objects, making implementation even more nuanced.

To simplify this, we’ve introduced **AIS-ETL Web Server Framework** in both **Go** and **Python**. These SDKs abstract away the boilerplate—so you can build and deploy custom ETL containers in minutes. Focus solely on your transformation logic; the SDK handles everything else, including networking, protocol handling, and high-throughput optimizations.

### Getting Started

* [Python SDK Quickstart (FastAPI, Flask, HTTP Multi-threaded)](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/webserver/README.md#quickstart)
* [Go Web Server Framework Guide](https://github.com/NVIDIA/aistore/blob/main/ext/etl/webserver/README.md)
* Examples: [Python](https://github.com/NVIDIA/aistore/blob/main/python/aistore/sdk/etl/webserver/README.md#examples) | [Go](https://github.com/NVIDIA/aistore/blob/main/ext/etl/webserver/examples)

## Direct Put Optimization: Faster Bucket-to-Bucket ETL Transformation

In offline transformations, the destination target for a transformed object usually differs from the original target. By default, the ETL container sends the transformed data back to the original source target, which then forwards it to the destination. The **direct put** optimization streamlines this flow by allowing the ETL container to send the transformed object directly to the destination target.

![AIStore ETL Direct Put Optimization](/assets/ais_etl_series/etl-direct-put-optimization.png)

## Benchmark: FFmpeg Audio Transformation

To benchmark the performance of AIStore's ETL framework and its optimized data movement capabilities, an audio transformation pipeline was implemented using **FFmpeg**, one of the most widely adopted multimedia processing tools. The transformer converts `.flac` audio files to `.wav` format, with configurable control over **Audio Channels** (`AC`) and **Audio Rate** (`AR`).

The motivation:

1. **Practicality** – FFmpeg is widely used across open-source communities and within NVIDIA Speech teams.
2. **Relevance** – Audio transformation is a byte-level, compute-intensive task, ideal for end-to-end ETL benchmarking.

Three Python-based web server implementations—**FastAPI**, **Flask**, and a multi-threaded HTTP server—were used to expose FFmpeg as a transformation service. Each configuration was tested with and without `direct_put` and `fqn`.

### Setup and Configuration

#### System

* **Kubernetes Cluster**: 3 bare-metal Nodes (each running one [proxy](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#proxy) and one [target](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#target))
* **Storage**: 10 HDDs per target (9.1 TiB each)
* **CPU**: 48 cores per node
* **Memory**: 187 GiB per node

#### Transformer and Scripts

* [FFmpeg Transformer Source](https://github.com/NVIDIA/ais-etl/tree/main/transformers/FFmpeg)
* [Benchmark Script](https://github.com/NVIDIA/ais-etl/blob/main/transformers/tests/test_ffmpeg.py)

#### Dataset

The [LibriSpeech (500 Hours)](https://www.openslr.org/resources/12/train-other-500.tar.gz) dataset was used for evaluation. It contains approximately 149,000 `.flac` audio files. Each file was converted to **stereo** at **44,100 Hz** and saved as `.wav`.

### Benchmarks

#### Client-Side Transformation (Baseline)

The current state of the art is executing data pre-processing, augmentation, and a wide variety of custom ETL workflows on individual client machines. Client-side transformation is the conventional approach in object storage, where data is downloaded, processed locally, and then re-uploaded. This test used a client with the same configuration as AIStore nodes, connected within the same VCN. It used **24 threads** to pull, transform, and push data across buckets.

* **Total time:** 2 hours 15 minutes 6 seconds
* [Local Benchmark Script](https://github.com/NVIDIA/ais-etl/blob/main/transformers/tests/local_benchmark/ffmpeg_benchmark.py)

#### AIStore ETL Benchmarks

Benchmarks were performed across all three Python-based web servers: **FastAPI**, **Flask**, and **HTTP**. Each combination was tested with and without `direct_put` and `fqn`.

The chart below summarizes the performance across configurations:

![benchmark](/assets/ais_etl_series/etl-benchmark.png)

| Webserver   | Communication Type | FQN   | Direct Put   |   Seconds |
|:------------|:-------|:------|:------------:|----------:|
| fastapi     | ws     | fqn   | True         |   218.541 |
| flask       | hpush  | fqn   | False        |   258.171 |
| flask       | hpull  | fqn   | False        |   258.868 |
| fastapi     | hpush  |       | True         |   259.963 |
| fastapi     | hpush  | fqn   | True         |   260.094 |
| flask       | hpull  | fqn   | True         |   261.366 |
| flask       | hpush  | fqn   | True         |   261.658 |
| fastapi     | hpush  |       | False        |   262.539 |
| fastapi     | hpull  | fqn   | True         |   262.666 |
| fastapi     | hpull  | fqn   | False        |   263.526 |
| fastapi     | hpush  | fqn   | False        |   266.175 |
| flask       | hpush  |       | False        |   270.632 |
| fastapi     | hpull  |       | True         |   270.923 |
| flask       | hpush  |       | True         |   272.426 |
| flask       | hpull  |       | False        |   275.475 |
| fastapi     | hpull  |       | False        |   277.491 |
| flask       | hpull  |       | True         |   277.512 |
| fastapi     | ws     | fqn   | True         |   298.657 |
| http        | hpush  |       | False        |   419.697 |
| http        | hpull  | fqn   | False        |   441.644 |
| http        | hpush  | fqn   | False        |   444.687 |
| http        | hpush  |       | True         |   636.434 |
| http        | hpush  | fqn   | True         |   652.89  |
| http        | hpull  |       | False        |   906.59  |
| http        | hpull  |       | True         |  1070.84  |

The best-performing configuration was the **FastAPI server** with both `FQN` and `Direct Put` enabled, completing the transformation in **3 minutes 39 seconds**.

> **Note:** AIStore ETL performance scales linearly with additional nodes and disks. Increasing cluster size further improves transformation throughput.

This result represents a **\~35x speedup** compared to conventional client-side object storage transformations.
