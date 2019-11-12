# AIStore: scalable storage for AI

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Go Report Card](https://goreportcard.com/badge/github.com/NVIDIA/aistore)

AIStore (AIS for short) is a built from scratch, lightweight storage stack tailored for AI apps. At its version 2.x, AIS consistently shows balanced I/O distribution across arbitrary numbers of clustered servers and hard drives (consistently) producing performance charts that look as follows:

<img src="docs/images/ais-disk-throughput-flat.png" alt="I/O distribution" width="600">

> The picture above *comprises* 120 HDDs.

> The capability to linearly scale-out for millions of stored objects (often also referred to as *shards*) was, and remains, one of the main incentives to build AIStore. But not the only one.

Further, AIS:

* scales-out with no downtime and no limitation;
* supports n-way mirroring (RAID-1), m/k erasure coding, end-to-end data protection;
* includes Map/Reduce extension to speed-up shuffle/resize for large datasets;
* runs on commodity hardware with no limitations and no special requirements;
* provides a proper subset of S3-like REST API;
* leverages Linux 4.15+ storage stack;
* natively supports Amazon S3 and Google Cloud backends;
* focuses on AI and, specifically, on the performance of large-scale deep learning.

Last but not the least, AIS features open format and, therefore, freedom to copy or move your data off of AIS at any time using familiar Linux `scp(1)` and `rsync(1)`. For general description, design philosophy, key concepts, and system components, please see [AIS overview](docs/overview.md).

## Table of Contents
- [Prerequisites](#prerequisites)
- [Getting Started](#getting-started)
    - [Local non-Containerized](#local-non-containerized)
    - [Local Docker-Compose](#local-docker-compose)
    - [Local Kubernetes](#local-kubernetes)
- [Containerized Deployment and Host Resource Sharing](#containerized-deployment-and-host-resource-sharing)
    - [Performance Monitoring](#performance-monitoring)
- [Guides and References](#guides-and-references)
- [Selected Package READMEs](#selected-package-readmes)

## Prerequisites

* Linux (with `gcc`, `sysstat` and `attr` packages, and kernel 4.15+)
* [Go 1.13 or later](https://golang.org/dl/)
* Extended attributes (`xattrs` - see below)
* Optionally, Amazon (AWS) or Google Cloud Platform (GCP) account(s)

Depending on your Linux distribution you may or may not have `gcc`, `sysstat`, and/or `attr` packages.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or xattrs, is a long time POSIX legacy and is supported by all mainstream filesystems with no exceptions. Unfortunately, extended attributes (xattrs) may not always be enabled (by the Linux distribution you are using) in the Linux kernel configurations - the fact that can be easily found out by running `setfattr` command.

> If disabled, please make sure to enable xattrs in your Linux kernel configuration.

## Getting Started

AIStore runs on commodity Linux machines with no special hardware requirements.

> It is expected, though, that all AIS target machines are identical, hardware-wise.

The implication is that the number of [possible deployment options](deploy) is practically unlimited.
This section covers 3 (three) ways to deploy AIS on a single Linux machine and is intended for developers and development, and/or for a quick trial.

### Local non-Containerized

Assuming that [Go](https://golang.org/dl/) is already installed, the remaining getting-started steps are:

```shell
$ cd $GOPATH/src
$ go get -v github.com/NVIDIA/aistore/ais
$ cd github.com/NVIDIA/aistore/ais
$ make deploy
$ go test ./tests -v -run=Mirror
```
The `go get` command installs AIS sources and all the versioned dependencies under your configured [$GOPATH](https://golang.org/cmd/go/#hdr-GOPATH_environment_variable).

The `make deploy` command deploys AIStore daemons locally based on a few prompted Q&A. The example shown below deploys 10 targets (each with 2 local simulated filesystems) and 3 gateways, and will not require (or expect) to access Cloud storage (notice the "Cloud Provider" prompt below):

```shell
# make deploy
Enter number of storage targets:
10
Enter number of proxies (gateways):
3
Number of local cache directories (enter 0 to use preconfigured filesystems):
2
Select Cloud Provider:
1: Amazon Cloud
2: Google Cloud
3: None
Enter your choice:
3
```

Or, you can run all of the above with a single command:

```shell
# make kill; ./setup/deploy.sh <<< $'10\n3\n2\n3'
```

> `make kill` will terminate local AIStore if it's already running.

> To enable optional AIStore authentication server, execute instead `$ CREDDIR=/tmp/creddir AUTHENABLED=true make deploy`. For information on AuthN server, please see [AuthN documentation](authn/README.md).

Finally, the `go test` (above) will create an ais bucket, configure it as a two-way mirror, generate thousands of random objects, read them all several times, and then destroy the replicas and eventually the bucket as well.

Alternatively, if you happen to have Amazon and/or Google Cloud account, make sure to specify the corresponding bucket name when running `go test` For example, the following will download objects from your (presumably) S3 bucket and distribute them across AIStore:

```shell
$ BUCKET=myS3bucket go test ./tests -v -run=download
```

Here's a minor variation of the above:

```shell
$ BUCKET=myS3bucket go test ./tests -v -run=download -args -numfiles=100 -match='a\d+'
```

This command runs test that matches the specified string ("download"). The test then downloads up to 100 objects from the bucket called myS3bucket, whereby the names of those objects match `a\d+` regex.

> In addition to the AIS cluster itself you can deploy [AIS CLI](cli/README.md) - an easy-to-use AIS-integrated command-line management tool. The tool supports multiple commands and options; the first one that you may want to try is `ais status` to show state and status of the AIS cluster and its nodes. AIS CLI deployment is documented in the [CLI readme](cli/README.md) and includes two easy steps: building the binary (via `cli/install.sh`) and sourcing Bash auto-completions.

> For more testing commands and command line options, please refer to the corresponding [README](ais/tests/README.md) and/or the [test sources](ais/tests/).
> For other useful commands, see the [Makefile](ais/Makefile).

> For tips and help pertaining to local non-containerized deployment, please see [the tips](docs/local_tips.md).

> For info on how to run AIS executables, see [command-line arguments](docs/command_line.md).

> For helpful links and background on Go, AWS, GCP, and Deep Learning, please see [helpful links](docs/helpful-links.md).

### Local Docker-Compose

The 2nd option to run AIS on your local machine requires [Docker](https://docs.docker.com/) and [Docker-Compose](https://docs.docker.com/compose/overview/). It also allows for multi-clusters deployment with multiple separate networks. You can deploy a simple AIS cluster within seconds or deploy a multi-container cluster for development.

> AIS v2.1 supports up to 3 (three) logical networks: user (or public), intra-cluster control and intra-cluster data networks.

To get started with AIStore and Docker, see: [Getting started with Docker](docs/docker_main.md).

### Local Kubernetes

The 3rd and final local-deployment option makes use of [Kubeadm](https://kubernetes.io/docs/reference/setup-tools/kubeadm/kubeadm/) and is documented [here](deploy/dev/kubernetes).

## Containerized Deployment and Host Resource Sharing

The following **applies to all containerized deployments**: local and non-local - the latter including those that are "kubernetized".

1. AIS nodes always automatically detect *containerization*.
2. If deployed as a container, each AIS node independently discovers whether its own container's memory and/or CPU resources are restricted.
3. Finally, the node then abides by those restrictions.

To that end, each AIS node at startup loads and parses [cgroup](https://www.kernel.org/doc/Documentation/cgroup-v2.txt) settings for the container and, if the number of CPUs is restricted, adjusts the number of allocated system threads for its goroutines.

> This adjustment is accomplished via the Go runtime [GOMAXPROCS variable](https://golang.org/pkg/runtime/). For in-depth information on CPU bandwidth control and scheduling in a multi-container environment, please refer to the [CFS Bandwidth Control](https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt) document.

Further, given the container's cgroup/memory limitation, each AIS node adjusts the amount of memory available for itself. Note, however, that memory in particular may affect dSort and erasure coding performance "forcing" those two to, effectively, "spill" their temporary content onto local drives, etc.

> For technical details on AIS memory management, please see [this readme](memsys/README.md).

### Performance Monitoring

As is usually the case with storage clusters, there are multiple ways to monitor their performance.

> AIStore includes `aisloader` - the tool to stress-test and benchmark storage performance. For background, command-line options and usage, please see [AIS Load Generator](docs/howto_benchmark.md).

For starters, AIS collects and logs a fairly large and constantly growing number of counters that describe all aspects of its operation including (but not limited to) those that reflect cluster recovery/rebalancing, all [extended long-running operations](docs/xaction.md), and, of course, object storage transactions.

In particular:

> For dSort monitoring, please see [dSort](dsort/README.md)
> For Downloader monitoring, please see [Internet Downloader](downloader/README.md)

The logging interval is called `stats_time` (default `10s`) and is [configurable](docs/configuration.md) on the level of both each specific node and the entire cluster.

However. Speaking of ways to monitor AIS remotely, the two most obvious ones would be:

* [AIS CLI](cli/README.md)
* Graphite/Grafana

As far as Graphite/Grafana, AIS integrates with these popular backends via [StatsD](https://github.com/etsy/statsd) - the *daemon for easy but powerful stats aggregation*. StatsD can be connected to Graphite which then can be used as a data-source for Grafana to get visual overview of the statistics and metrics.

> The scripts for easy deployment of both Graphite and Grafana are included (see below).

> For [local non-containerized deployments](#local-non-containerized), use `./ais/setup/deploy_grafana.sh` to start Graphite and Grafana containers. Local deployment will automatically notice the presence of the containers and will send statistics to the Graphite.

> For [local docker-compose based deployments](#local-docker-compose), make sure to use `-grafana` command-line option. The `deploy_docker.sh` script will then spin-up Graphite and Grafana containers.

In both of these cases, Grafana will be accessible at [localhost:3000](http://localhost:3000).

> For information on AIS statistics, please see [Statistics, Collected Metrics, Visualization](docs/metrics.md)

## Guides and References
- [AIS: Detailed Overview](docs/overview.md)
- [Command line parameters](docs/command_line.md)
- [AIS Load Generator (integrated benchmark tool)](bench/aisloader/README.md)
- [Batch List and Range Operations: Prefetch, and more](docs/batch.md)
- [Object checksums: Brief Theory of Operations](docs/checksum.md)
- [Configuration](docs/configuration.md)
- [Datapath: Read and Write Sequence Diagrams](docs/datapath.md)
- [Highly available control plane](docs/ha.md)
- [How to benchmark](docs/howto_benchmark.md)
- [RESTful API](docs/http_api.md)
- [Joining AIS cluster](docs/join_cluster.md)
- [Bucket (definition, operations, properties)](docs/bucket.md#bucket)
- [Statistics, Collected Metrics, Visualization](docs/metrics.md)
- [Performance Tuning and Testing](docs/performance.md)
- [Cluster-wide Rebalancing](docs/rebalance.md)
- [Storage Services](docs/storage_svcs.md)
- [Extended Actions](docs/xaction.md)
- [Integrated Internet Downloader](downloader/README.md)
- [Docker for AIS developers](docs/docker_main.md)
- [Experimental](docs/experimental.md)

## Selected Package READMEs
- [Package `downloader`](downloader/README.md)
- [Package `memsys`](memsys/README.md)
- [Package `transport`](transport/README.md)
- [Package `api`](api/README.md)
- [Package `dSort`](dsort/README.md)
- [Package `openapi`](openapi/README.md)
