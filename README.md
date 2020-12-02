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
* [ETL offload](/etl/README.md): running user-defined extract-transform-load workloads on (and by) performance optimized storage cluster;

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

- [Prerequisites](#prerequisites)
- [Local Playground](#local-playground)
- [Build, Make, and Development Tools](#build-make-and-development-tools)
- [Deployment](#deployment)
- [Containerized Deployments: Host Resource Sharing](#containerized-deployments-host-resource-sharing)
- [Performance Monitoring](#performance-monitoring)
- [Configuration](#configuration)
- [Amazon S3 compatibility](docs/s3compat.md)
- [TensorFlow integration](docs/tensorflow.md)
- [Guides and References](#guides-and-references)
- [Assorted Tips](#assorted-tips)
- [Selected Package READMEs](#selected-package-readmes)

## Prerequisites

AIStore runs on commodity Linux machines with no special hardware requirements whatsoever.
Deployment [options](deploy) are practically unlimited and include a spectrum with bare-metal (Kubernetes) clusters of any size, on the one hand, and a single Linux or Mac host, on the other.

> It is expected, though, that within a given cluster all AIS target machines are identical, hardware-wise.

* [Linux](#Linux) (with `gcc`, `sysstat` and `attr` packages, and kernel 4.15+) or [MacOS](#MacOS)
* [Go 1.15 or later](https://golang.org/dl/)
* Extended attributes (`xattrs` - see below)
* Optionally, Amazon (AWS) or Google Cloud Platform (GCP) account(s)

### Linux

Depending on your Linux distribution, you may or may not have `gcc`, `sysstat`, and/or `attr` packages.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or xattrs, is a long time POSIX legacy and is supported by all mainstream filesystems with no exceptions. Unfortunately, extended attributes (xattrs) may not always be enabled (by the Linux distribution you are using) in the Linux kernel configurations - the fact that can be easily found out by running `setfattr` command.

> If disabled, please make sure to enable xattrs in your Linux kernel configuration.

### MacOS

MacOS/Darwin is also supported, albeit for development only. Certain capabilities related to querying the state-and-status of local hardware resources (memory, CPU, disks) may be missing, which is why we **strongly** recommend Linux for production deployments.

## Local Playground

> For production deployments on Kubernetes, please refer to a separate dedicated [github repo](https://github.com/NVIDIA/ais-k8s).

> For local production deployment, please refer to this [README](/deploy/prod/docker/single/README.md).

> For easy steps to run local playground on [GCP](https://cloud.google.com), please refer to this [README](/deploy/dev/terraform/README.md).

Assuming that [Go](https://golang.org/dl/) toolchain is already installed, the steps to deploy AIS locally on a single development machine are:

```console
$ cd $GOPATH/src
$ go get -v github.com/NVIDIA/aistore/ais
$ cd github.com/NVIDIA/aistore
$ make deploy
$ go test ./tests -v -run=Mirror
```

where:

* `go get` installs sources and dependencies under your [$GOPATH](https://golang.org/cmd/go/#hdr-GOPATH_environment_variable).
* `make deploy` deploys AIStore daemons locally and interactively, for example:

```console
$ make deploy
Enter number of storage targets:
10
Enter number of proxies (gateways):
3
Number of local cache directories (enter 0 to use preconfigured filesystems):
2
Select cloud providers:
Amazon S3: (y/n) ?
n
Google Cloud Storage: (y/n) ?
n
Azure: (y/n) ?
n
Would you like to create loopback mount points: (y/n) ?
n
Building aisnode: version=df24df77 providers=
```

Or, you can run all the above in one shot non-interactively:

```console
$ make kill deploy <<< $'10\n3\n2\nn\nn\nn\nn\n'
```

> The example deploys 3 gateways and 10 targets, each with 2 local simulated filesystems.
> Also notice the "Cloud" prompt above, and the fact that access to 3rd party Cloud storage is a deployment-time option.

> `make kill` will terminate local AIStore if it's already running.


For more development options and tools, please refer to [development docs](docs/development.md).

Finally, the `go test` (above) will create an AIS bucket, configure it as a two-way mirror, generate thousands of random objects, read them all several times, and then destroy the replicas and eventually the bucket as well.

Alternatively, if you happen to have Amazon and/or Google Cloud account, make sure to specify the corresponding (S3 or GCS) bucket name when running `go test` commands.
For example, the following will download objects from your (presumably) S3 bucket and distribute them across AIStore:

```console
$ BUCKET=aws://myS3bucket go test ./tests -v -run=download
```

### HTTPS

In the end, all examples above run a bunch of local web servers that listen for plain HTTP requests. Following are quick steps for developers to engage HTTPS:

1. Generate X.509 certificate:

```console
$ openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 1080 -nodes -subj '/CN=localhost'
```

2. Deploy cluster (4 targets, 1 gateway, 6 mountpaths, Google Cloud):

```console
$ AIS_USE_HTTPS=true AIS_SKIP_VERIFY_CRT=true make kill deploy <<< $'4\n1\n6\nn\ny\nn\n'
```

3. Run tests (both examples below list the names of buckets accessible for you in Google Cloud):

```console
$ AIS_ENDPOINT=https://localhost:8080 AIS_SKIP_VERIFY_CRT=true BUCKET=gs://myGCPbucket go test -v -p 1 -count 1 ./ais/tests -run=BucketNames

$ AIS_ENDPOINT=https://localhost:8080 AIS_SKIP_VERIFY_CRT=true BUCKET=tmp go test -v -p 1 -count 1 ./ais/tests -run=BucketNames
```

> Notice environment variables above: **AIS_USE_HTTPS**, **AIS_ENDPOINT**, and **AIS_SKIP_VERIFY_CRT**.

## Build, Make and Development Tools

As noted, the project utilizes GNU `make` to build and run things both locally and remotely (e.g., when deploying AIStore via [Kubernetes](deploy/dev/k8s/Dockerfile). As the very first step, run `make help` for help on:

* **building** AIS binary (called `aisnode`) deployable as both a storage target **or** a proxy/gateway;
* **building** [CLI](cmd/cli/README.md), [aisfs](cmd/aisfs/README.md), and benchmark binaries;

In particular, the `make` provides a growing number of developer-friendly commands to:

* **deploy** AIS cluster on your local development machine;
* **run** all or selected tests;
* **instrument** AIS binary with race detection, CPU and/or memory profiling, and more.

## Deployment

AIStore can be easily deployed on any bare-metal or virtualized hardware.
This repository contains all the scripts needed to run AIS on your laptop or Linux workstation.
For production deployments on Kubernetes, please refer to a separate dedicated github repo:

* [Deploying AIStore on K8s](https://github.com/NVIDIA/ais-k8s/blob/master/docs/README.md)
* [Deploying AIStore on cloud K8s](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md).
  It supports single-command infrastructure and software deployment.
  
  <img src="docs/images/ais-k8s-deploy.gif" alt="Kubernetes cloud deployment" width="70%">

The rest of this section talks about a single Linux machine and, as such, is intended for developers and development, *or* for a quick trial.

### Local Docker-Compose

[Local Playground](#local-playground) is probably the speediest option to run AIS clusters. However, to take advantage of containerization (which includes, for instance, multiple logically-isolated configurable networks), you can also run AIStore as described here:

* [Getting started with Docker](docs/docker_main.md).

<!-- videoStart
{% include_relative docker_videos.md %}
videoEnd -->

### Local Kubernetes

Yet another local-deployment option makes use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and is documented [here](deploy/dev/k8s/README.md).

## Containerized Deployments: Host Resource Sharing

The following **applies to all containerized deployments**:

1. AIS nodes always automatically detect *containerization*.
2. If deployed as a container, each AIS node independently discovers whether its own container's memory and/or CPU resources are restricted.
3. Finally, the node then abides by those restrictions.

To that end, each AIS node at startup loads and parses [cgroup](https://www.kernel.org/doc/Documentation/cgroup-v2.txt) settings for the container and, if the number of CPUs is restricted, adjusts the number of allocated system threads for its goroutines.

> This adjustment is accomplished via the Go runtime [GOMAXPROCS variable](https://golang.org/pkg/runtime/). For in-depth information on CPU bandwidth control and scheduling in a multi-container environment, please refer to the [CFS Bandwidth Control](https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt) document.

Further, given the container's cgroup/memory limitation, each AIS node adjusts the amount of memory available for itself.

> Limits on memory may affect [dSort](dsort/README.md) performance forcing it to "spill" the content associated with in-progress resharding into local drives. The same is true for erasure-coding that also requires memory to rebuild objects from slices, etc.

> For technical details on AIS memory management, please see [this readme](memsys/README.md).

## Performance Monitoring

As is usually the case with storage clusters, there are multiple ways to monitor their performance.

> AIStore includes `aisloader` - the tool to stress-test and benchmark storage performance. For background, command-line options, and usage, please see [Load Generator](bench/aisloader/README.md) and [How To Benchmark AIStore](docs/howto_benchmark.md).

For starters, AIS collects and logs a fairly large and constantly growing number of counters that describe all aspects of its operation, including (but not limited to) those that reflect cluster recovery/rebalancing, all [extended long-running operations](xaction/README.md), and, of course, object storage transactions.

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
