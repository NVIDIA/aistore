---
layout: post
title: AIStore - scalable storage for AI applications
permalink: /
redirect_from:
 - /index.md/
 - /README.md/
---

AIStore (AIS) is a lightweight distributed storage stack tailored for AI applications. It's an elastic cluster that can grow and shrink at runtime and can be ad-hoc deployed, with or without Kubernetes, anywhere from a single Linux machine to a bare-metal cluster of any size. Built from scratch, AIS provides linear scale-out across storage nodes, consistent performance, and a flexible deployment model.

![License](https://img.shields.io/badge/license-MIT-blue.svg)

AIS consistently shows balanced I/O distribution and linear scalability across arbitrary numbers of clustered nodes. The system supports fast data access, reliability, and rich customization for data transformation workloads.

## Features

* ✅ **Multi-Cloud Access:** Seamlessly access and manage content across multiple [cloud backends](/docs/overview.md#at-a-glance) (including AWS S3, GCS, Azure, OCI), with an additional benefit of fast-tier performance and configurable data redundancy.
* ✅ **Deploy Anywhere:** AIS runs on any Linux machine, virtual or physical. Deployment options range from a single [Docker container](https://github.com/NVIDIA/aistore/blob/main/deploy/prod/docker/single/README.md) and [Google Colab](https://aistore.nvidia.com/blog/2024/09/18/google-colab-aistore) to petascale [Kubernetes clusters](https://github.com/NVIDIA/ais-k8s). There are [no limitations](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#no-limitations-principle) on deployment or functionality.
* ✅ **High Availability:** Redundant control and data planes. Self-healing, end-to-end protection, n-way mirroring, and erasure coding. Arbitrary number of lightweight access points.
* ✅ **HTTP-based API:** A feature-rich, native API (with user-friendly SDKs for Go and Python), and compliant [Amazon S3 API](/docs/s3compat.md) for running unmodified S3 clients.
* ✅ **Unified Namespace:** Attach AIS clusters together to provide fast, unified access to the entirety of hosted datasets, allowing users to reference shared buckets with cluster-specific identifiers.
* ✅ **Turn-key Cache:** In addition to robust data protection features, AIS offers a per-bucket configurable LRU-based cache with eviction thresholds and storage capacity watermarks.
* ✅ **Custom ETL Offload:** Execute I/O intensive data transformations close to the data, either inline (on-the-fly as part of each read request) or offline (batch processing, with the destination bucket populated with transformed results).
* ✅ **Existing File Datasets:** Ingest file datasets from any local or remote source, either on-demand (ad-hoc) or through asynchronous [batch](/docs/overview.md#promote-local-or-shared-files).
* ✅ **Read-after-Write Consistency:** Guaranteed [consistency](/docs/overview.md#read-after-write consistency) across all gateways.
* ✅ **Write-through:** In presence of remote backends, AIS executes remote writes as part of the same [wriiting transaction](/docs/overview.md#write-through).
* ✅ **Small File Optimization:** AIS supports TAR, ZIP, TAR.GZ, and TAR.LZ4 serialization for batching and processing small files. Supported features include [initial sharding](https://aistore.nvidia.com/blog/2024/08/16/ishard), distributed shuffle (re-sharding), appending to existing shards, listing contained files, and [more](/docs/overview.md#shard).
* ✅ **Kubernetes**. For production deployments, we developed the [AIS/K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator). A dedicated GitHub [repository](https://github.com/NVIDIA/ais-k8s) contains Ansible scripts, Helm charts, and deployment guidance.
* ✅ **Authentication and Access Control:** OAuth 2.0-compatible [authentication server (AuthN)](/docs/authn.md).
* ✅ **Batch Jobs:** Start, monitor, and control cluster-wide [batch operations](/docs/batch.md).

The feature set is actively growing and also includes: [adding/removing nodes at runtime](/docs/lifecycle_node.md), managing [TLS certificates](/docs/cli/x509.md) at runtime, listing, copying, prefetching, and transforming [virtual directories](/docs/howto_virt_dirs.md), executing [presigned S3 requests](/docs/s3compat.md#presigned-s3-requests), adaptive [rate limiting](/docs/rate_limit.md), and more.

> For the original **white paper** and design philosophy, please see [AIStore Overview](/docs/overview.md), which also includes high-level block diagram, terminology, APIs, CLI, and more.
> For our 2024 KubeCon presentation, please see [AIStore: Enhancing petascale Deep Learning across Cloud backends](https://www.youtube.com/watch?v=N-d9cbROndg).

## CLI

AIS includes an integrated, scriptable [CLI](/docs/cli.md) for managing clusters, buckets, and objects, running and monitoring batch jobs, viewing and downloading logs, generating performance reports, and more:

```console
$ ais <TAB-TAB>

advanced         config           get              prefetch         show
alias            cp               help             put              space-cleanup
archive          create           job              remote-cluster   start
auth             download         log              rmb              stop
blob-download    dsort            ls               rmo              storage
bucket           etl              object           scrub            tls
cluster          evict            performance      search           wait
```

## Developer Tools

AIS runs natively on Kubernetes and features open format - thus, the freedom to copy or move your data from AIS at any time using the familiar Linux `tar(1)`, `scp(1)`, `rsync(1)` and similar.

For developers and data scientists, there's also:

* [Go API](https://github.com/NVIDIA/aistore/tree/main/api) used in [CLI](/docs/cli.md) and [benchmarking tools](/docs/aisloader.md)
* [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk) + [Reference Guide](/docs/python_sdk.md)
* [PyTorch integration](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch) and usage examples
* [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch)

## Quick Start

1. Read the [Getting Started Guide](/docs/getting_started.md) for a 5-minute local install, or
2. Run a [minimal](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/docker/single) AIS cluster consisting of a single gateway and a single storage node, or
3. Clone the repo and run `make kill cli aisloader deploy` followed by `ais show cluster`

---------------------

## Deployment options

AIS deployment options, as well as intended (development vs. production vs. first-time) usages, are all [summarized here](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md).

Since the prerequisites essentially boil down to having Linux with a disk the deployment options range from [all-in-one container](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/docker/single) to a petascale bare-metal cluster of any size, and from a single VM to multiple racks of high-end servers. Practical use cases require, of course, further consideration.

Some of the most popular deployment options include:

| Option | Use Case |
| --- | ---|
| [Local playground](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#local-playground) | AIS developers or first-time users, Linux or Mac OS. Run `make kill cli aisloader deploy <<< $'N\nM'`, where `N` is a number of [targets](/docs/overview.md#target), `M` - [gateways](/docs/overview.md#proxy) |
| Minimal production-ready deployment | This option utilizes preinstalled docker image and is targeting first-time users or researchers (who could immediately start training their models on smaller datasets) |
| [Docker container](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/docker/single) | Quick testing and evaluation; single-node setup |
| [GCP/GKE automated install](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#kubernetes-deployments) | Developers, first-time users, AI researchers |
| [Large-scale production deployment](https://github.com/NVIDIA/ais-k8s) | Requires Kubernetes; provided via [ais-k8s](https://github.com/NVIDIA/ais-k8s) |

> For performance tuning, see [performance](/docs/performance.md) and [AIS K8s Playbooks](https://github.com/NVIDIA/ais-k8s/tree/main/playbooks/host-config).

## Existing Datasets

AIS supports multiple ingestion modes:

* ✅ **On Demand:** Transparent cloud access during workloads.
* ✅ **PUT:** Locally accessible files and directories.
* ✅ **Promote:** Import local target directories and/or NFS/SMB shares mounted on AIS targets.
* ✅ **Copy:** Full buckets, virtual subdirectories, and lists or ranges (via Bash expansion).
* ✅ **Download:** HTTP(S)-accessible datasets and objects.
* ✅ **Prefetch:** Remote buckets or selected objects (from remote buckets), including subdirectories, lists, and/or ranges.
* ✅ **Archive:** [Group and store](https://aistore.nvidia.com/blog/2024/08/16/ishard) related small files from an original dataset.

## Install from Release Binaries

You can install the CLI and benchmarking tools using:

```console
./scripts/install_from_binaries.sh --help
```

The script installs [aisloader](/docs/aisloader.md) and [CLI](/docs/cli.md) from the latest or previous GitHub [release](https://github.com/NVIDIA/aistore/releases) and enables CLI auto-completions.

## PyTorch integration

PyTorch integration is a growing set of datasets (both iterable and map-style), samplers, and dataloaders:

* [Taxonomy of abstractions and API reference](/docs/pytorch.md)
* [AIS plugin for PyTorch: usage examples](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch/README.md)
* [Jupyter notebook examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/aisio-pytorch/)

## Guides and References (extended index)

- [Getting Started](/docs/getting_started.md)
- [Technical Blog](https://aistore.nvidia.com/blog)
- API and SDK
  - [Go (language) API](https://github.com/NVIDIA/aistore/tree/main/api)
  - [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore), and also:
    - [pip package](https://pypi.org/project/aistore/)
    - [reference guide](/docs/python_sdk.md)
  - [REST API](/docs/http_api.md)
    - [Easy URL](/docs/easy_url.md)
- Amazon S3
  - [`s3cmd` client](/docs/s3cmd.md)
  - [S3 compatibility](/docs/s3compat.md)
  - [Presigned S3 requests](/docs/s3compat.md#presigned-s3-requests)
  - [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch)
- [CLI](/docs/cli.md)
  - [`ais help`](/docs/cli/help.md)
  - [Reference guide](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md#cli-reference)
  - [Monitoring](/docs/cli/show.md)
    - [`ais show cluster`](/docs/cli/show.md)
    - [`ais performance`](/docs/cli/performance.md)
    - [`ais show job`](/docs/cli/show.md)
  - [Cluster and node management](/docs/cli/cluster.md)
  - [Mountpath (disk) management](/docs/cli/storage.md)
  - [Attach, detach, and monitor remote clusters](/docs/cli/cluster.md)
  - [Start, stop, and monitor downloads](/docs/cli/download.md)
  - [Distributed shuffle](/docs/cli/dsort.md)
  - [User account and access management](/docs/cli/auth.md)
  - [Jobs](/docs/cli/job.md)
- Security and Access Control
  - [Authentication Server (AuthN)](/docs/authn.md)
- Archiving and Sharding
  - [Reading, writing, and listing *archives*](/docs/archive.md)
  - [Initial Sharding of Machine Learning Datasets](https://aistore.nvidia.com/blog/2024/08/16/ishard)
  - [Distributed Shuffle](/docs/dsort.md)
- Power tools and extensions
  - [Downloader](/docs/downloader.md)
  - [Distributed Shuffle](/docs/dsort.md)
  - [Extract, Transform, Load](/docs/etl.md)
  - [Tools and utilities](/docs/tools.md)
- Benchmarking and tuning Performance
  - [AIS Load Generator: integrated benchmark tool](/docs/aisloader.md)
  - [How to benchmark](/docs/howto_benchmark.md)
  - [Performance tuning and testing](/docs/performance.md)
  - [Performance monitoring](/docs/cli/performance.md)
- Buckets and Backend Providers
  - [Backend providers](/docs/providers.md)
  - [Buckets](/docs/bucket.md)
- Storage Services
  - [CLI: `ais show storage` and subcommands](/docs/cli/show.md)
  - [CLI: `ais storage` and subcommands](/docs/cli/storage.md)
  - [Storage Services](/docs/storage_svcs.md)
  - [Checksumming: brief theory of operations](/docs/checksum.md)
  - [S3 compatibility](/docs/s3compat.md)
- Cluster Management
  - [Node lifecycle: maintenance mode, rebalance/rebuild, shutdown, decommission](/docs/lifecycle_node.md)
  - [Monitoring: `ais show` and subcommands](/docs/cli/show.md)
  - [Joining AIS cluster](/docs/join_cluster.md)
  - [Leaving AIS cluster](/docs/leave_cluster.md)
  - [Global Rebalance](/docs/rebalance.md)
  - [Troubleshooting](/docs/troubleshooting.md)
- Configuration
  - [Configuration](/docs/configuration.md)
  - [Environment variables](/docs/environment-vars.md)
  - [CLI: `ais config`](/docs/cli/config.md)
  - [Feature flags](/docs/feature_flags.md)
- Observability
  - [Prometheus](/docs/prometheus.md)
    - [Reference: all supported metrics](/docs/metrics-reference.md)
  - [Observability overview: StatsD and Prometheus, logs, and CLI](/docs/metrics.md)
  - [CLI: `ais show performance`](/docs/cli/show.md)
- For users and developers
  - [Getting started](/docs/getting_started.md)
  - [Docker](/docs/docker_main.md)
  - [Useful scripts](/docs/development.md)
  - Profiling, race-detecting and more
- Batch jobs
  - [Batch operations](/docs/batch.md)
  - [eXtended Actions (xactions)](https://github.com/NVIDIA/aistore/blob/main/xact/README.md)
  - [CLI: `ais job`](/docs/cli/job.md) and [`ais show job`](/docs/cli/show.md), including:
    - [prefetch remote datasets](/docs/cli/object.md#prefetch-objects)
    - [copy (list, range, and/or prefix) selected objects or entire (in-cluster or remote) buckets](/docs/cli/bucket.md#copy-list-range-andor-prefix-selected-objects-or-entire-in-cluster-or-remote-buckets)
    - [download remote BLOBs](/docs/cli/blob-downloader.md)
    - [promote NFS or SMB share](https://aistore.nvidia.com/blog/2022/03/17/promote)
- Assorted Topics
  - [Virtual directories](/docs/howto_virt_dirs.md)
  - [System files](/docs/sysfiles.md)
  - [HTTPS: loading, reloading, and generating certificates; switching cluster between HTTP and HTTPS](/docs/https.md)
    - [Managing TLS Certificates](/docs/cli/x509.md)
  - [Feature flags](/docs/feature_flags.md)
  - [`aisnode` command line](/docs/command_line.md)
  - [Traffic patterns](/docs/traffic_patterns.md)
  - [Highly available control plane](/docs/ha.md)
  - [Start/stop maintenance mode, shutdown, decommission, and related operations](/docs/lifecycle_node.md)
  - [Buckets: definition, operations, properties](https://github.com/NVIDIA/aistore/blob/main/docs/bucket.md#bucket)
  - [Out-of-band updates](/docs/out_of_band.md)
  - [CLI: Three Ways to Evict Remote Bucket](/docs/cli/evicting_buckets_andor_data.md)
  - [Using Rate Limits to Enhance Reliability and Performance](/docs/rate_limit.md)

## License

MIT

## Author

Alex Aizman (NVIDIA)
