**AIStore is a lightweight object storage system with the capability to linearly scale out with each added storage node and a special focus on petascale deep learning.**

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Report Card](https://goreportcard.com/badge/github.com/NVIDIA/aistore)

AIStore (AIS for short) is a built from scratch, lightweight storage stack tailored for AI apps. AIS consistently shows balanced I/O distribution and linear scalability across arbitrary numbers of clustered servers, producing performance charts that look as follows:

<img src="/docs/images/ais-disk-throughput-flat.png" alt="I/O distribution" width="600">

> The picture above *comprises* 120 HDDs.

The ability to scale linearly with each added disk was, and remains, one of the main incentives behind AIStore. Much of the development is also driven by the ideas to [offload dataset transformations](https://aiatscale.org/blog/2021/10/21/ais-etl-1) to AIS clusters.

## Features

* scale out with no downtime and no limitation;
* arbitrary number of extremely lightweight access points;
* highly-available control and data planes, end-to-end data protection, self-healing, n-way mirroring, k/m erasure coding;
* comprehensive native HTTP-based (S3-like) API, as well as
  * compliant [Amazon S3 API](/docs/s3compat.md) to run unmodified S3 clients and apps;
* automated cluster rebalancing upon any changes in cluster membership, drive failures and attachments, bucket renames;
* [ETL offload](/docs/etl.md) via offline (dataset to dataset) or inline (on-the-fly) transformations.

Also, AIStore:

* can be deployed on any commodity hardware - effectively, on any Linux machine(s);
* can be immediately populated - i.e., hydrated - from any file-based data source (local or remote, ad-hoc/on-demand or via asynchronus batch);
* provides for easy Kubernetes deployment via a separate GitHub repo with
  * step-by-step [deployment playbooks](https://github.com/NVIDIA/ais-k8s/tree/master/playbooks), and
  * [AIS/K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/master/operator);
* contains integrated [CLI](/docs/cli.md) for easy management and monitoring;
* can ad-hoc attach remote AIS clusters, thus gaining immediate access to the respective hosted datasets
  * (referred to as [global namespace](/docs/providers.md#remote-ais-cluster) capability);
* natively reads, writes, and lists [popular archives](/docs/cli/archive.md) including tar, tar.gz, zip, and [MessagePack](https://msgpack.org);
  * [distributed shuffle](/docs/dsort.md) of those archival formats is also supported;
* fully supports Amazon S3, Google Cloud, and Microsoft Azure backends
  * providing [unified global namespace](/docs/bucket.md) simultaneously across multiple backends:

<img src="docs/images/backends.png" alt="Supported Backends" width="360">

* can be deployed as LRU-based fast cache for remote buckets; can be populated on-demand and/or via `prefetch` and `download` APIs;
* can be used as a standalone highly-available protected storage;
* includes MapReduce extension for massively parallel resharding of very large datasets;
* supports existing [PyTorch](https://storagetarget.files.wordpress.com/2019/12/deep-learning-large-scale-phys-poster-1.pdf) and [TensorFlow](https://www.youtube.com/watch?v=r9uw1BNt2x8&feature=youtu.be)-based training models.

AIS runs natively on Kubernetes and features open format - thus, the freedom to copy or move your data from AIS at any time using the familiar Linux `tar(1)`, `scp(1)`, `rsync(1)` and similar.

For AIStore **white paper** and design philosophy, for introduction to large-scale deep learning and the most recently added features, please see [AIStore Overview](/docs/overview.md) (where you can also find six alternative ways to work with existing datasets). Videos and **animated presentations** can be found at [videos](/docs/videos.md).

Finally, [getting started](/docs/getting_started.md) with AIS takes only a few minutes.

## Deployment options

The prerequisites boil down to having Linux with a disk. The result is a (practically unlimited) set of options ranging from [all-in-one container](/docs/videos.md#minimal-all-in-one-standalone-docker) to a petascale bare-metal cluster of any size, and from single VM to multiple racks of high-end servers.

Contents of this repository, intended (development vs. production vs. first-time, etc.) usages and further references are all [summarized here](deploy/README.md).

Use cases include:

| Deployment option | Targeted audience and objective |
| --- | ---|
| [Local playground](/docs/getting_started.md#local-playground) | AIS developers and development, Linux or Mac OS |
| Minimal production-ready deployment | This option utilizes preinstalled docker image and is targeting first-time users or researchers (who could immediately start training their models on smaller datasets) |
| [Easy automated GCP/GKE deployment](/docs/getting_started.md#kubernetes-deployments) | Developers, first-time users, AI researchers |
| [Large-scale production deployment](https://github.com/NVIDIA/ais-k8s) | Requires Kubernetes and is provided via a separate repository: [ais-k8s](https://github.com/NVIDIA/ais-k8s) |

Further, there's the capability referred to as [global namespace](/docs/providers.md#remote-ais-cluster): given HTTP(S) connectivity, AIS clusters can be easily interconnected to "see" each other's datasets. Hence, the idea to start "small" to gradually and incrementally build high-performance shared capacity.

> For detailed discussion on supported deployments, please refer to [Getting Started](/docs/getting_started.md).

> For performance tuning and preparing AIS nodes for bare-metal deployment, see [performance](/docs/performance.md).

## Related Software

When it comes to PyTorch, [WebDataset](https://github.com/webdataset/webdataset) is the preferred AIStore client.

> WebDataset is a PyTorch Dataset (IterableDataset) implementation providing efficient access to datasets stored in POSIX tar archives.

Further references include technical blog titled [AIStore & ETL: Using WebDataset to train on a sharded dataset](https://aiatscale.org/blog/2021/10/29/ais-etl-3) where you can also find easy step-by-step instruction.

## Guides and References

- [Getting Started](/docs/getting_started.md)
- [Technical Blog](https://aiatscale.org/blog)
- API
  - [Native RESTful API](/docs/http_api.md)
  - [S3 compatibility](/docs/s3compat.md)
  - [Go API/SDK](/docs/http_api.md)
- [CLI](/docs/cli.md)
  - [Create, destroy, list, copy, rename, transform, configure, evict buckets](/docs/cli/bucket.md)
  - [GET, PUT, APPEND, PROMOTE, and other operations on objects](/docs/cli/object.md)
  - [Cluster and node management](/docs/cli/cluster.md)
  - [Mountpath (disk) management](/docs/cli/storage.md)
  - [Attach, detach, and monitor remote clusters](/docs/cli/cluster.md)
  - [Start, stop, and monitor downloads](/docs/cli/download.md)
  - [Distributed shuffle](/docs/cli/dsort.md)
  - [User account and access management](/docs/cli/auth.md)
  - [Job (aka `xaction`) management](/docs/cli/job.md)
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
- Buckets and Backend Providers
  - [Backend providers](/docs/providers.md)
  - [Buckets](/docs/bucket.md)
- Storage Services
  - [Storage Services](/docs/storage_svcs.md)
  - [Checksumming: brief theory of operations](/docs/checksum.md)
  - [S3 compatibility](/docs/s3compat.md)
- Cluster Management
  - [Joining AIS cluster](/docs/join_cluster.md)
  - [Leaving AIS cluster](/docs/leave_cluster.md)
  - [Global Rebalance](/docs/rebalance.md)
  - [Troubleshooting](/docs/troubleshooting.md)
- Configuration
  - [Configuration](/docs/configuration.md)
  - [CLI to view and update cluster and node config](/docs/cli/config.md)
- Observability
  - [Observability](/docs/metrics.md)
  - [Prometheus](/docs/prometheus.md)
- For developers
  - [Getting started](/docs/getting_started.md)
  - [Docker](/docs/docker_main.md)
  - [Useful scripts](/docs/development.md)
  - Profiling, race-detecting, and more
- Batch operations
  - [Batch operations](/docs/batch.md)
  - [eXtended Actions (xactions)](/xaction/README.md)
  - [CLI: job management](/docs/cli/job.md)
- Topics
  - [System files](/docs/sysfiles.md)
  - [`aisnode` command line](/docs/command_line.md)
  - [Traffic patterns](/docs/traffic_patterns.md)
  - [Highly available control plane](/docs/ha.md)
  - [File access](/docs/aisfs.md)
  - [Downloader](/docs/downloader.md)
  - [On-disk layout](/docs/on_disk_layout.md)
  - [AIS Buckets: definition, operations, properties](/docs/bucket.md#bucket)

## License

MIT

## Author

Alex Aizman (NVIDIA)
