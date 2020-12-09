**AIStore is a lightweight object storage system with the capability to linearly scale-out with each added storage node and a special focus on petascale deep learning.**

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Report Card](https://goreportcard.com/badge/github.com/NVIDIA/aistore)

AIStore (AIS for short) is a built from scratch, lightweight storage stack tailored for AI apps. AIS consistently shows balanced I/O distribution and linear scalability across arbitrary numbers of clustered servers, producing performance charts that look as follows:

<img src="docs/images/ais-disk-throughput-flat.png" alt="I/O distribution" width="600">

> The picture above *comprises* 120 HDDs.

The ability to scale linearly with each added disk was, and remains, one of the main incentives behind AIStore. Much of the development is also driven by the ideas to offload dataset transformation and other I/O intensive stages of the ETL pipelines.

## Features

* scale-out with no downtime and no limitation;
* comprehensive HTTP REST API to GET and PUT objects, create, destroy, list and configure buckets, and more;
* [Amazon S3 API](https://docs.aws.amazon.com/s3/index.html) to run unmodified S3 apps;
* FUSE client (`aisfs`) to access AIS objects as files;
* arbitrary number of extremely lightweight access points;
* easy-to-use CLI that supports [TAB auto-completions](cmd/cli/README.md);
* automated cluster rebalancing upon: changes in cluster membership, drive failures and attachments, bucket renames;
* N-way mirroring (RAID-1), Reed–Solomon erasure coding, end-to-end data protection.
* [ETL offload](/docs/etl.md): running user-defined extract-transform-load workloads on (and by) performance-optimized storage cluster;

Also, AIStore:

* can be deployed on any commodity hardware;
* supports single-command infrastructure and software deployment on Google Cloud Platform via [ais-k8s GitHub repo](https://github.com/NVIDIA/ais-k8s);
* supports Amazon S3, Google Cloud, and Microsoft Azure backends (and all S3, GCS, and Azure-compliant object storages);
* provides unified global namespace across (ad-hoc) connected AIS clusters;
* can be used as a fast cache for GCS and S3; can be populated on-demand and/or via `prefetch` and `download` APIs;
* can be used as a standalone highly-available protected storage;
* includes MapReduce extension for massively parallel resharding of very large datasets;
* supports existing [PyTorch](https://storagetarget.files.wordpress.com/2019/12/deep-learning-large-scale-phys-poster-1.pdf) and [TensorFlow](https://www.youtube.com/watch?v=r9uw1BNt2x8&feature=youtu.be)-based training models.

Last but not least, AIS runs natively on Kubernetes and features open format and, therefore, freedom to copy or move your data off of AIS at any time using familiar Linux `tar(1)`, `scp(1)`, `rsync(1)` and similar.

For AIStore **white paper** and design philosophy, for introduction to large-scale deep learning and the most recently added features, please see [AIStore Overview](docs/overview.md) (where you can also find six alternative ways to work with existing datasets).

**Table of Contents**

- [Introduction](#introduction)
- [Monitoring](#monitoring)
- [Configuration](#configuration)
- [Amazon S3 compatibility](docs/s3compat.md)
- [TensorFlow integration](docs/tensorflow.md)
- [Guides and References](#guides-and-references)
- [Assorted Tips](#assorted-tips)
- [Selected Package READMEs](#selected-package-readmes)

## Introduction

AIStore supports numerous deployment options covering a spectrum from a single-laptop to petascale bare-metal clusters of any size. This includes:


| Deployment option | Targeted audience and objective |
| --- | ---|
| [Local playground](docs/getting_started.md#local-playground-and-development-deployment) | AIS developers and development, Linux or Mac OS |
| Minimal production-ready deployment | This option utilizes preinstalled docker image and is targeting first-time users or researchers (who could immediately start training their models on smaller datasets) |
| [Easy automated GCP/GKE deployment](docs/getting_started.md#cloud-deployment) | Developers, first-time users, AI researchers |
| [Large-scale production deployment](https://github.com/NVIDIA/ais-k8s) | Requires Kubernetes and is provided (documented, automated) via a separate repository: [ais-k8s](https://github.com/NVIDIA/ais-k8s) |

For detailed information on these and other supported options, and for a step-by-step instruction, please refer to [Getting Started](/docs/getting_started.md).

## Monitoring

As is usually the case with storage clusters, there are multiple ways to monitor their performance.

> AIStore includes `aisloader` - the tool to stress-test and benchmark storage performance. For background, command-line options, and usage, please see [Load Generator](bench/aisloader/README.md) and [How To Benchmark AIStore](docs/howto_benchmark.md).

For starters, AIS collects and logs a fairly large and growing number of counters that describe all aspects of its operation, including (but not limited to) those that reflect cluster recovery/rebalancing, all [extended long-running operations](xaction/README.md), and, of course, object storage transactions.

In particular:

* For dSort monitoring, please see [dSort](dsort/README.md)
* For Downloader monitoring, please see [Internet Downloader](downloader/README.md)

The logging interval is called `stats_time` (default `10s`) and is [configurable](docs/configuration.md) on the level of both each specific node and the entire cluster.

However. Speaking of ways to monitor AIS remotely, the two most obvious ones would be:

* [AIS CLI](cmd/cli/README.md)
* Graphite/Grafana

As far as Graphite/Grafana, AIS integrates with these popular backends via [StatsD](https://github.com/etsy/statsd) - the *daemon for easy but powerful stats aggregation*. StatsD can be connected to Graphite, which then can be used as a data source for Grafana to get a visual overview of the statistics and metrics.

> The scripts for easy deployment of both Graphite and Grafana are included (see below).

> For local non-containerized deployments, use `./deploy/dev/local/deploy_grafana.sh` to start Graphite and Grafana containers.
> Local deployment scripts will automatically "notice" the presence of the containers and will send statistics to the Graphite.

> For local docker-compose based deployments, make sure to use `-grafana` command-line option.
> The `./deploy/dev/docker/deploy_docker.sh` script will then spin-up Graphite and Grafana containers.

In both of these cases, Grafana will be accessible at [localhost:3000](http://localhost:3000).

> For information on AIS statistics, please see [Statistics, Collected Metrics, Visualization](docs/metrics.md)

## Configuration

AIS configuration is consolidated in a single [JSON template](/deploy/dev/local/aisnode_config.sh) where the configuration sections and the knobs within those sections must be self-explanatory, whereby the majority of those (except maybe just a few) have pre-assigned default values. The configuration template serves as a **single source for all deployment-specific configurations**, examples of which can be found under the folder that consolidates both [containerized-development and production deployment scripts](deploy).

AIS production deployment, in particular, requires careful consideration of at least some of the configurable aspects. For example, AIS supports 3 (three) logical networks and will, therefore, benefit, performance-wise, if provisioned with up to 3 isolated physical networks or VLANs. The logical networks are:

* user (aka public)
* intra-cluster control
* intra-cluster data

with the corresponding [JSON names](/deploy/dev/local/aisnode_config.sh), respectively:

* `ipv4`
* `ipv4_intra_control`
* `ipv4_intra_data`

## Assorted Tips

* To enable an optional AIStore authentication server, execute `$ AUTH_ENABLED=true make deploy`. For information on AuthN server, please see [AuthN documentation](cmd/authn/README.md).
* In addition to AIStore - the storage cluster, you can also deploy [aisfs](cmd/aisfs/README.md) - to access AIS objects as files, and [AIS CLI](cmd/cli/README.md) - to monitor, configure and manage AIS nodes and buckets.
* AIS CLI is an easy-to-use command-line management tool supporting a growing number of commands and options (one of the first ones you may want to try could be `ais show cluster` - show the state and status of an AIS cluster). The CLI is documented in the [readme](cmd/cli/README.md); getting started with it boils down to running `make cli` and following the prompts.
* For more testing commands and options, please refer to the [testing README](ais/tests/README.md).
* For `aisnode` command-line options, see: [command-line options](docs/command_line.md).
* For helpful links and/or background on Go, AWS, GCP, and Deep Learning: [helpful links](docs/helpful-links.md).
* And again, run `make help` to find out how to build, run, and test AIStore and tools.

## Guides and References

- [AIS Overview](docs/overview.md)
- [Playbooks](docs/playbooks/README.md)
- [CLI](cmd/cli/README.md)
  - [Create, destroy, list, and other operations on buckets](cmd/cli/resources/bucket.md)
  - [GET, PUT, APPEND, PROMOTE, and other operations on objects](cmd/cli/resources/object.md)
  - [Cluster and Node management](cmd/cli/resources/daeclu.md)
  - [Mountpath (Disk) management](cmd/cli/resources/mpath.md)
  - [Attach, Detach, and monitor remote clusters](cmd/cli/resources/remote.md)
  - [Start, Stop, and monitor downloads](cmd/cli/resources/download.md)
  - [Distributed Sort](cmd/cli/resources/dsort.md)
  - [User account and access management](cmd/cli/resources/users.md)
  - [Xaction (Job) management](cmd/cli/resources/xaction.md)
- [ETL with AIStore](docs/etl.md)
- [On-Disk Layout](docs/on-disk-layout.md)
- [System Files](docs/sysfiles.md)
- [Command line parameters](docs/command_line.md)
- [AIS Load Generator: integrated benchmark tool](bench/aisloader/README.md)
- [Batch List and Range Operations: Prefetch, and more](docs/batch.md)
- [Object checksums: Brief Theory of Operations](docs/checksum.md)
- [Configuration](docs/configuration.md)
- [Traffic patterns](docs/traffic_patterns.md)
- [Highly available control plane](docs/ha.md)
- [How to benchmark](docs/howto_benchmark.md)
- [RESTful API](docs/http_api.md)
- [FUSE with AIStore](cmd/aisfs/README.md)
- [Joining AIS cluster](docs/join_cluster.md)
- [Removing a node from AIS cluster](docs/leave_cluster.md)
- [AIS Buckets: definition, operations, properties](docs/bucket.md#bucket)
- [Statistics, Collected Metrics, Visualization](docs/metrics.md)
- [Performance: Tuning and Testing](docs/performance.md)
- [Rebalance](docs/rebalance.md)
- [Storage Services](docs/storage_svcs.md)
- [Extended Actions](xaction/README.md)
- [Integrated Internet Downloader](downloader/README.md)
- [Docker for AIS developers](docs/docker_main.md)
- [Troubleshooting Cluster Operation](docs/troubleshooting.md)

## Selected Package READMEs

- [Package `api`](api/README.md)
- [Package `cli`](cmd/cli/README.md)
- [Package `fuse`](cmd/aisfs/README.md)
- [Package `downloader`](downloader/README.md)
- [Package `memsys`](memsys/README.md)
- [Package `transport`](transport/README.md)
- [Package `dSort`](dsort/README.md)
- [Package `openapi`](openapi/README.md)

## License

MIT

## Author

Alex Aizman (NVIDIA)
