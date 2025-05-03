**AIStore: High-Performance, Scalable Storage for AI Workloads**

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Report Card](https://goreportcard.com/badge/github.com/NVIDIA/aistore)

AIStore (AIS) is a lightweight distributed storage stack tailored for AI applications. Built from scratch, AIS provides linear scale-out across storage nodes, consistent performance, and a flexible deployment model.

AIS consistently shows balanced I/O distribution and linear scalability across arbitrary numbers of clustered nodes. The system supports fast data access, reliability, and rich customization for data transformation workloads.

## Features

* ✅ **Universal Multi-Cloud Access:** Seamlessly access and manage content across multiple [cloud backends](/docs/overview.md#at-a-glance) (including AWS S3, GCS, Azure, OCI), with an additional benefit of fast-tier performance and configurable data redundancy.
* ✅ **Deploy Anywhere:** AIS runs on any Linux machine, virtual or physical. Deployment options range from a single [Docker container](https://github.com/NVIDIA/aistore/blob/main/deploy/prod/docker/single/README.md) and [Google Colab](https://aistore.nvidia.com/blog/2024/09/18/google-colab-aistore) to petascale [Kubernetes clusters](https://github.com/NVIDIA/ais-k8s). There are [no limitations](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#no-limitations-principle) on deployment or functionality.
* ✅ **High Availability:** Redundant control and data planes. Self-healing, end-to-end protection, n-way mirroring, and erasure coding. Arbitrary number of lightweight access points.
* ✅ **HTTP-based API:** A feature-rich, native API (with user-friendly SDKs for Go and Python), and compliant [Amazon S3 API](/docs/s3compat.md) for running unmodified S3 clients.
* ✅ **Unified Namespace:** Attach AIS clusters together to provide fast, unified access to the entirety of hosted datasets, allowing users to reference shared buckets with cluster-specific identifiers.
* ✅ **Turn-key Cache:** In addition to robust data protection features, AIS offers a per-bucket configurable LRU-based cache with eviction thresholds and storage capacity watermarks.
* ✅ **Custom ETL Offload:** Execute I/O intensive data transformations close to the data, either inline (on-the-fly as part of each read request) or offline (batch processing, with the destination bucket populated with transformed results).
* ✅ **Existing File Datasets:** Ingest file datasets from any local or remote source, either on-demand (ad-hoc) or through asynchronous [batch](/docs/overview.md#promote-local-or-shared-files).
* ✅ **Read-after-Write Consistency:** Once the first object replica is finalized, subsequent reads are guaranteed to view the same content, with operations performed through any AIS gateway. Additional copies or erasure-coded slices are added asynchronously, if configured.
* ✅ **Write-through:** In presence of remote backends, AIS executes remote writes (using the vendor's SDK) as part of the same [transaction](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#read-after-write-consistency) that finalizes storing the object's first replica in the cluster. (If the remote write fails, the entire write operation will also fail.)
* ✅ **Small File Optimization:** AIS supports TAR, ZIP, TAR.GZ, and TAR.LZ4 formats for batching and processing small files. Supported features include resharding for optimal sorting, appending to existing shards, and listing contained files.
* ✅ **Kubernetes**. For production deployments, we developed the [AIS/K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator). A dedicated GitHub [repository](https://github.com/NVIDIA/ais-k8s) contains Ansible scripts, Helm charts, and deployment guidance.
* ✅ **Authentication and Access Control:** OAuth 2.0-compatible [authentication server (AuthN)](/docs/authn.md).
* ✅ **Distributed Shuffle:** Efficiently reshard large datasets in parallel.
* ✅ **Batch Jobs:** Start, monitor, and control cluster-wide [batch operations](/docs/batch.md).

For ease of use, management, and monitoring, there's also:
* **Integrated easy-to-use [CLI](/docs/cli.md)**, with top-level commands including:

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

AIS runs natively on Kubernetes and features open format - thus, the freedom to copy or move your data from AIS at any time using the familiar Linux `tar(1)`, `scp(1)`, `rsync(1)` and similar.

For developers and data scientists, there's also:
* native [Go (language) API](https://github.com/NVIDIA/aistore/tree/main/api) that we utilize in a variety of tools including [CLI](/docs/cli.md) and [Load Generator](/docs/aisloader.md);
* native [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk)
  - [Python SDK reference guide](/docs/python_sdk.md)
* [PyTorch integration](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch) and usage examples
* [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch) for interoperability with AWS SDK for Python (aka [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)) client
  - and other [Botocore](https://github.com/boto/botocore) derivatives.

For the original AIS **white paper** and design philosophy, for introduction to large-scale deep learning and the most recently added features, please see [AIStore Overview](/docs/overview.md) (where you can also find six alternative ways to work with existing datasets). For our 2024 KubeCon presentation, please see [AIStore: Enhancing petascale Deep Learning across Cloud backends](https://www.youtube.com/watch?v=N-d9cbROndg).

Finally, [getting started](/docs/getting_started.md) with AIS takes only a few minutes.

---------------------

## Deployment options

AIS deployment options, as well as intended (development vs. production vs. first-time) usages, are all [summarized here](deploy/README.md).

Since the prerequisites essentially boil down to having Linux with a disk the deployment options range from [all-in-one container](https://github.com/NVIDIA/aistore/tree/main/deploy/prod/docker/single) to a petascale bare-metal cluster of any size, and from a single VM to multiple racks of high-end servers. Practical use cases require, of course, further consideration.

Some of the most popular deployment options include:

| Option | Objective |
| --- | ---|
| [Local playground](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#local-playground) | AIS developers or first-time users, Linux or Mac OS; to get started, run `make kill cli aisloader deploy <<< $'N\nM'`, where `N` is a number of [targets](/docs/overview.md#terminology), `M` - [gateways](/docs/overview.md#terminology) |
| Minimal production-ready deployment | This option utilizes preinstalled docker image and is targeting first-time users or researchers (who could immediately start training their models on smaller datasets) |
| [Easy automated GCP/GKE deployment](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#kubernetes-deployments) | Developers, first-time users, AI researchers |
| [Large-scale production deployment](https://github.com/NVIDIA/ais-k8s) | Requires Kubernetes and is provided via a separate repository: [ais-k8s](https://github.com/NVIDIA/ais-k8s) |

Further, there's the capability referred to as [global namespace](https://github.com/NVIDIA/aistore/blob/main/docs/providers.md#remote-ais-cluster): given HTTP(S) connectivity, AIS clusters can be easily interconnected to "see" each other's datasets. Hence, the idea to start "small" to gradually and incrementally build high-performance shared capacity.

> For detailed discussion on supported deployments, please refer to [Getting Started](/docs/getting_started.md).

> For performance tuning and preparing AIS nodes for bare-metal deployment, see [performance](/docs/performance.md).

## Existing datasets

AIS supports multiple ways to populate itself with existing datasets, including (but not limited to):

* **on demand**, often during the first epoch;
* **copy** entire bucket or its selected virtual subdirectories;
* **copy** multiple matching objects;
* **archive** multiple objects
* **prefetch** remote bucket or parts of thereof;
* **download** raw http(s) addressable directories, including (but not limited to) Cloud storages;
* **promote** NFS or SMB shares accessible by one or multiple (or all) AIS [target](/docs/overview.md#terminology) nodes;

> The on-demand "way" is maybe the most popular, whereby users just start running their workloads against a [remote bucket](docs/providers.md) with AIS cluster positioned as an intermediate fast tier.

But there's more. In [v3.22](https://github.com/NVIDIA/aistore/releases/tag/v1.3.22), we introduce [blob downloader](/docs/blob_downloader.md), a special facility to download very large remote objects (BLOBs). And in [v3.23](https://github.com/NVIDIA/aistore/releases/tag/v1.3.23), there's a new capability, dubbed [bucket inventory](/docs/s3inventory.md), to list very large S3 buckets _fast_.

## Installing from release binaries

Generally, AIS (cluster) requires at least some sort of [deployment](/deploy#contents) procedure. There are standalone binaries, though, that can be [built](Makefile) from source or installed directly from GitHub:

```console
$ ./scripts/install_from_binaries.sh --help
```

The script installs [aisloader](/docs/aisloader.md) and [CLI](/docs/cli.md) from the most recent, or the previous, GitHub [release](https://github.com/NVIDIA/aistore/releases). For CLI, it'll also enable auto-completions (which is strongly recommended).

## PyTorch integration

PyTorch integration is a growing set of datasets (both iterable and map-style), samplers, and dataloaders:

* [Taxonomy of abstractions and API reference](/docs/pytorch.md)
* [AIS plugin for PyTorch: usage examples](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch/README.md)
* [Jupyter notebook examples](https://github.com/NVIDIA/aistore/tree/main/python/examples/aisio-pytorch/)

Since AIS natively supports [remote backends](/docs/providers.md), you can also use (PyTorch + AIS) to iterate over Amazon S3, GCS, Azure, and OCI buckets, and more.

## AIStore Badge

Let others know your project is powered by high-performance AI storage:

[![aistore](https://img.shields.io/badge/powered%20by-AIStore-76B900?style=flat&labelColor=000000)](https://github.com/NVIDIA/aistore)

```markdown
[![aistore](https://img.shields.io/badge/powered%20by-AIStore-76B900?style=flat&labelColor=000000)](https://github.com/NVIDIA/aistore)
```

## Guides and References

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
    - [`ais show performance`](/docs/cli/show.md)
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
- Power tools and extensions
  - [Reading, writing, and listing *archives*](/docs/archive.md)
  - [Distributed Shuffle](/docs/dsort.md)
  - [Downloader](/docs/downloader.md)
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
  - [Distributed Tracing](/docs/distributed-tracing.md)
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
