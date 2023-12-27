**AIStore is a lightweight object storage system with the capability to linearly scale out with each added storage node and a special focus on petascale deep learning.**

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Report Card](https://goreportcard.com/badge/github.com/NVIDIA/aistore)

AIStore (AIS for short) is a built from scratch, lightweight storage stack tailored for AI apps. It's an elastic cluster that can grow and shrink at runtime and can be ad-hoc deployed, with or without Kubernetes, anywhere from a single Linux machine to a bare-metal cluster of any size.

AIS consistently shows balanced I/O distribution and **linear scalability** across arbitrary numbers of clustered nodes. The ability to scale linearly with each added disk was, and remains, one of the main incentives. Much of the initial design was also driven by the ideas to [offload](https://aiatscale.org/blog) custom dataset transformations (often referred to as [ETL](https://aiatscale.org/blog/2021/10/21/ais-etl-1)). And finally, since AIS is a software system that aggregates Linux machines to provide storage for user data, there's the requirement number one: reliability and data protection.

## Features

* **Deploys anywhere**. AIS clusters are immediately deployable on any commodity hardware, on any Linux machine(s).
* **Highly available** control and data planes, end-to-end data protection, self-healing, n-way mirroring, erasure coding, and arbitrary number of extremely lightweight access points.
* **REST API**. Comprehensive native HTTP-based API, as well as compliant [Amazon S3 API](/docs/s3compat.md) to run unmodified S3 clients and apps.
* **Unified namespace** across multiple [remote backends](/docs/providers.md) including Amazon S3, Google Cloud, and Microsoft Azure.
* **Network of clusters**. Any AIS cluster can attach any other AIS cluster, thus gaining immediate visibility and fast access to the respective hosted datasets.
* **Turn-key cache**. Can be used as a standalone highly-available protected storage and/or LRU-based fast cache. Eviction watermarks, as well as numerous other management policies, are per-bucket configurable.
* **ETL offload**. The capability to run I/O intensive custom data transformations *close to data* - offline (dataset to dataset) and inline (on-the-fly).
* **File datasets**. AIS can be immediately populated from any file-based data source (local or remote, ad-hoc/on-demand or via asynchronus batch).
* **Read-after-write consistency**. Reading and writing (as well as all other control and data plane operations) can be performed via any (random, selected, or load-balanced) AIS gateway (a.k.a. "proxy"). Once the first replica of an object is written and _finalized_ subsequent reads are guaranteed to view the same content. Additional copies and/or EC slices, if configured, are added asynchronously via `put-copies` and `ec-put` jobs, respectively.
* **Write-through**. In presence of any [remote backend](/docs/providers.md), AIS executes remote write (e.g., using vendor's SDK) as part of the [transaction](https://github.com/NVIDIA/aistore/blob/main/docs/overview.md#read-after-write-consistency) that places and _finalizes_ the first replica.
* **Small file datasets.** To serialize small files and facilitate batch processing, AIS supports TAR, TAR.GZ (or TGZ), ZIP, and TAR.LZ4 formatted objects (often called _shards_). Resharding (for optimal sorting and sizing), listing contained files (samples), appending to existing shards, and generating new ones from existing objects and/or client-side files - is also fully supported.
* **Kubernetes**. Provides for easy Kubernetes deployment via a separate GitHub [repo](https://github.com/NVIDIA/ais-k8s) and [AIS/K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/master/operator).
* **Command line management**. Integrated powerful [CLI](/docs/cli.md) for easy management and monitoring.
* **Access control**. For security and fine-grained access control, AIS includes OAuth 2.0 compliant [Authentication Server (AuthN)](/docs/authn.md). A single AuthN instance executes CLI requests over HTTPS and can serve multiple clusters.
* **Distributed shuffle** extension for massively parallel resharding of very large datasets.
* **Batch jobs**. APIs and CLI to start, stop, and monitor documented [batch operations](/docs/batch.md), such as `prefetch`, `download`, copy or transform datasets, and many more.

AIS runs natively on Kubernetes and features open format - thus, the freedom to copy or move your data from AIS at any time using the familiar Linux `tar(1)`, `scp(1)`, `rsync(1)` and similar.

For developers and data scientists, there's also:
* native [Go (language) API](https://github.com/NVIDIA/aistore/tree/main/api) that we utilize in a variety of tools including [CLI](/docs/cli.md) and [Load Generator](/docs/aisloader.md);
* native [Python SDK](https://github.com/NVIDIA/aistore/tree/main/python/aistore/sdk)
  - [Python SDK reference guide](/docs/python_sdk.md)
* [PyTorch integration](https://github.com/NVIDIA/aistore/tree/main/python/aistore/pytorch) and usage examples
* [Boto3 support](https://github.com/NVIDIA/aistore/tree/main/python/aistore/botocore_patch) for interoperability with AWS SDK for Python (aka [Boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)) client
  - and other [Botocore](https://github.com/boto/botocorehttps://github.com/boto/botocore) derivatives.

For the original AIStore **white paper** and design philosophy, for introduction to large-scale deep learning and the most recently added features, please see [AIStore Overview](/docs/overview.md) (where you can also find six alternative ways to work with existing datasets). Videos and **animated presentations** can be found at [videos](/docs/videos.md).

Finally, [getting started](/docs/getting_started.md) with AIS takes only a few minutes.

---------------------

## Deployment options

AIS deployment options, as well as intended (development vs. production vs. first-time) usages, are all [summarized here](deploy/README.md).

Since prerequisites boil down to, essentially, having Linux with a disk the deployment options range from [all-in-one container](/docs/videos.md#minimal-all-in-one-standalone-docker) to a petascale bare-metal cluster of any size, and from a single VM to multiple racks of high-end servers. But practical use cases require, of course, further consideration and may include:

| Option | Objective |
| --- | ---|
| [Local playground](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#local-playground) | AIS developers and development, Linux or Mac OS |
| Minimal production-ready deployment | This option utilizes preinstalled docker image and is targeting first-time users or researchers (who could immediately start training their models on smaller datasets) |
| [Easy automated GCP/GKE deployment](https://github.com/NVIDIA/aistore/blob/main/docs/getting_started.md#kubernetes-deployments) | Developers, first-time users, AI researchers |
| [Large-scale production deployment](https://github.com/NVIDIA/ais-k8s) | Requires Kubernetes and is provided via a separate repository: [ais-k8s](https://github.com/NVIDIA/ais-k8s) |

Further, there's the capability referred to as [global namespace](https://github.com/NVIDIA/aistore/blob/main/docs/providers.md#remote-ais-cluster): given HTTP(S) connectivity, AIS clusters can be easily interconnected to "see" each other's datasets. Hence, the idea to start "small" to gradually and incrementally build high-performance shared capacity.

> For detailed discussion on supported deployments, please refer to [Getting Started](/docs/getting_started.md).

> For performance tuning and preparing AIS nodes for bare-metal deployment, see [performance](/docs/performance.md).

## Installing from release binaries

Generally, AIStore (cluster) requires at least some sort of [deployment](/deploy#contents) procedure. There are standalone binaries, though, that can be [built](Makefile) from source or, alternatively, installed directly from GitHub:

```console
$ ./deploy/scripts/install_from_binaries.sh --help
```

The script installs [aisloader](/docs/aisloader.md) and [CLI](/docs/cli.md) from the most recent, or the previous, GitHub [release](https://github.com/NVIDIA/aistore/releases). For CLI, it'll also enable auto-completions (which is strongly recommended).

## PyTorch integration

AIS is one of the PyTorch [Iterable Datapipes](https://github.com/pytorch/data/tree/main/torchdata/datapipes/iter/load#iterable-datapipes).

Specifically, [TorchData](https://github.com/pytorch/data) library provides:
* [AISFileLister](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLister.html#aisfilelister)
* [AISFileLoader](https://pytorch.org/data/main/generated/torchdata.datapipes.iter.AISFileLoader.html#aisfileloader)

to list and, respectively, load data from AIStore.

Further references and usage examples - in our technical blog at https://aiatscale.org/blog:
* [PyTorch: Loading Data from AIStore](https://aiatscale.org/blog/2022/07/12/aisio-pytorch)
* [Python SDK: Getting Started](https://aiatscale.org/blog/2022/07/20/python-sdk)

Since AIS natively supports a number of [remote backends](/docs/providers.md), you can also use (PyTorch + AIS) to iterate over Amazon S3 and Google Cloud buckets, and more.

## Reuse

This repo includes [SGL and Slab allocator](/memsys) intended to optimize memory usage, [Streams and Stream Bundles](/transport) to multiplex messages over long-lived HTTP connections, and a few other sub-packages providing rather generic functionality.

With a little effort, they all could be extracted and used outside.

## Guides and References

- [Getting Started](/docs/getting_started.md)
- [Technical Blog](https://aiatscale.org/blog)
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
- Tutorials
  - [Tutorials](/docs/tutorials/README.md)
  - [Videos](/docs/videos.md)
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
  - [CLI: `ais cluster` and subcommands](/docs/cli/show.md)
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
  - [Observability](/docs/metrics.md)
  - [Prometheus](/docs/prometheus.md)
  - [CLI: `ais show performance`](/docs/cli/show.md)
- For users and developers
  - [Getting started](/docs/getting_started.md)
  - [Docker](/docs/docker_main.md)
  - [Useful scripts](/docs/development.md)
  - Profiling, race-detecting, and more
- Batch jobs
  - [Batch operations](/docs/batch.md)
  - [eXtended Actions (xactions)](/xact/README.md)
  - [CLI: `ais job`](/docs/cli/job.md) and [`ais show job`](/docs/cli/show.md)
- Assorted Topics
  - [System files](/docs/sysfiles.md)
  - [Switching cluster between HTTP and HTTPS](/docs/switch_https.md)
  - [TLS: testing with self-signed certificates](/docs/getting_started.md#tls-testing-with-self-signed-certificates)
  - [Feature flags](/docs/feature_flags.md)
  - [`aisnode` command line](/docs/command_line.md)
  - [Traffic patterns](/docs/traffic_patterns.md)
  - [Highly available control plane](/docs/ha.md)
  - [Start/stop maintenance mode, shutdown, decommission, and related operations](/docs/lifecycle_node.md)
  - [Downloader](/docs/downloader.md)
  - [On-disk layout](/docs/on_disk_layout.md)
  - [Buckets: definition, operations, properties](https://github.com/NVIDIA/aistore/blob/main/docs/bucket.md#bucket)
  - [Out of band updates](/docs/validate_warm_get.md)

## License

MIT

## Author

Alex Aizman (NVIDIA)
