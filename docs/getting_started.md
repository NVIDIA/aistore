---
layout: post
title: GETTING STARTED
permalink: /docs/getting-started
redirect_from:
 - /getting_started.md/
 - /docs/getting_started.md/
---

AIStore runs on a single Linux or Mac machine. Bare-metal Kubernetes and [GCP/GKE](https://cloud.google.com/kubernetes-engine) Cloud-based deployments are also supported. There are numerous other options.

Generally, when deciding how to deploy a system like AIS with so many possibilities to choose from, a good place to start would be answering the following two fundamental questions:

* what's the dataset size, or sizes?
* what hardware will I use?

For datasets, say, below 50TB a single host may suffice and should, therefore, be considered a viable option. On the other hand, [Cloud deployment](#cloud-deployment) option may sound attractive for its ubiquitous convenience and for  _not_ thinking about the hardwares and the sizes - at least, not right away.

> Note as well that **you can always start small**: a single-host deployment, a 3-node cluster in the Cloud or on premises, etc. AIStore supports a number of options to inter-connect existing clusters - the capability called *unified global namespace* - or migrate existing datasets (on demand or via supported storage services). For introductions and further pointers, please refer to the [AIStore Overview](overview.md).

## Prerequisites

AIStore runs on commodity Linux machines with no special hardware requirements whatsoever.

> It is expected that within a given cluster, all AIS target machines are identical, hardware-wise.

* [Linux](#Linux) (with `gcc`, `sysstat` and `attr` packages, and kernel 4.15+) or [MacOS](#MacOS)
* [Go 1.17](https://golang.org/dl/) (and look for go1.17.9)
* Extended attributes (`xattrs` - see next section)
* Optionally, Amazon (AWS) or Google Cloud Platform (GCP) account(s)

### Linux

Depending on your Linux distribution, you may or may not have `gcc`, `sysstat`, and/or `attr` packages. These packages must be installed.

Speaking of distributions, our current default recommendations is Ubuntu Server 20.04 LTS. But Ubuntu 18.04 and CentOS 8.x (or later) will also work. As well as numerous others.

For the [local filesystem](performance.md), we currently recommend xfs. But again, this (default) recommendation shall not be interpreted as a limitation of any kind: other fine choices include zfs, ext4, f2fs, and more.

Since AIS itself provides n-way mirroring and erasure coding, hardware RAID would _not_ be recommended. But can be used, and will work.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or `xattrs`, is a long time POSIX legacy supported by all mainstream filesystems with no exceptions. Unfortunately, `xattrs` may not always be enabled in the Linux kernel configurations - the fact that can be easily found out by running `setfattr` command.

If disabled, please make sure to enable xattrs in your Linux kernel configuration. To quickly check:

```console
$ touch foo
$ setfattr -n user.bar -v ttt foo
$ getfattr -n user.bar foo
```

### MacOS

MacOS/Darwin is also supported, albeit for development only.
Certain capabilities related to querying the state-and-status of local hardware resources (memory, CPU, disks) may be missing, which is why we **strongly** recommend Linux for production deployments.

## Make

AIS comes with its own build system that we use in a variety of development and production environments. The very first `make` command to run would be:

```console
$ make help
```

The output - truncated below for the sake of brevity - includes supported subcommands, environment variables, and usage examples:

```console
Useful commands:
  aisfs                          Build 'aisfs' binary
  aisloader                      Build 'aisloader' binary
  all                            Build all main binaries
  authn                          Build 'authn' binary
  ci                             Run CI related checkers and linters (requires BUCKET variable to be set)
  clean-client-bindings          Remove all generated client binding files
  clean                          Remove all AIS related files and binaries
  cli-autocompletions            Add CLI autocompletions
  cli                            Build CLI ('ais' binary)
  deploy                         Build 'aisnode' and deploy the specified numbers of local AIS proxies and targets
  deploy-docker                  Deploy AIS cluster inside the dockers
...
...

Examples:
# Deploy cluster locally
$ make deploy

# Stop locally deployed cluster and cleanup all cluster-related data and bucket metadata (but not cluster map)
$ make kill clean

# Stop and then deploy (non-interactively) cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build `aisnode` executable with the support for GCP and AWS backends
$ make kill deploy <<< $'7\n4\n4\ny\ny\nn\nn\n'

# Shutdown and then (non-interactively) generate local configs and deploy a cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build executable with the support for GCP and AWS backends
$ make kill deploy <<< $'7\n4\n4\ny\ny\nn\nn\n'

# Restart a cluster of 7 targets (4 mountpaths each) and 2 proxies; utilize previously generated (pre-shutdown) local configurations
$ make restart <<< $'7\n4\n4\ny\ny\nn\nn\n'

# Redeploy (4 targets, 1 proxy) cluster; build executable for debug without any backend-supporting libraries; use RUN_ARGS to pass an additional command-line option ('-override_backends=true') to each running node
$ RUN_ARGS=-override_backends MODE=debug make kill deploy <<< $'4\n1\n4\nn\nn\nn\nn\n'

# Same as above, but additionally run all 4 targets in a standby mode
$ RUN_ARGS='-override_backends -standby' MODE=debug make kill deploy <<< $'4\n1\n4\nn\nn\nn\nn\n'
...
...
```

## Multiple deployment options

Are [all summarized and further referenced here](deploy/README.md).

In particular:

### Kubernetes Deployments

For any Kubernetes deployments (including, of course, production deployments) please use a separate and dedicated [AIS-K8s GitHub](https://github.com/NVIDIA/ais-k8s/blob/master/docs/README.md) repository. The repo contains [Helm Charts](https://github.com/NVIDIA/ais-k8s/tree/master/helm/ais/charts) and detailed [Playbooks](https://github.com/NVIDIA/ais-k8s/tree/master/playbooks) that cover a variety of use cases and configurations.

In particular, [AIS-K8s GitHub repository](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md) provides a single-line command to deploy Kubernetes cluster and the underlying infrastructure with AIStore cluster running inside (see below). The only requirement is having a few dependencies preinstalled (in particular, `helm`) and a Cloud account.

The following GIF illustrates steps to deploy AIS on Google Cloud Platform (GCP):

![Kubernetes cloud deployment](images/ais-k8s-deploy.gif)

Finally, the [repository](https://github.com/NVIDIA/ais-k8s) hosts [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/tree/master/operator) project that will eventually replace Helm charts and will become the main deployment, lifecycle, and operation management "vehicle" for AIStore.

### Minimal all-in-one-docker Deployment

This option has the unmatched convenience of requiring an absolute minimum time and resources - please see this [README](/deploy/prod/docker/single/README.md) for details.

## Local Playground

Running AIS locally is good for quick evaluation, experimenting with features, first-time usage, and, of course, development.

> Local AIStore playground is not intended for production clusters and is not meant to provide optimal performance.

To run it locally from the source, you have to have **Go** (compiler, linker, tools, system packages).

For Linux:

* download `go1.17.9.linux-amd64.tar.gz` from [Go downloads](https://golang.org/dl/)
* and follow [installation instructions](https://go.dev/doc/install)

> Note that Go 1.18 is _not_ supported yet. See [go.mod](/go.mod) for the most recently updated information.

Finally, if not done yet, export [`GOPATH`](https://go.dev/doc/gopath_code#GOPATH) environment variable.

At this point you should be able to run it, as follows:

### Steps to run AIS from source

Assuming that [Go](https://golang.org/dl/) toolchain is already installed, the steps to deploy AIS locally on a single development machine are:

```console
$ cd $GOPATH/src/github.com/NVIDIA
$ git clone https://github.com/NVIDIA/aistore.git
$ cd aistore
# optionally, run `make mod-tidy` to preload dependencies
$ ./deploy/scripts/clean_deploy.sh

$ ais show cluster
```
where:

* [`clean_deploy.sh`](/docs/development.md#clean-deploy) with no arguments builds AIStore binaries (such as `aisnode` and `ais` CLI)
and then deploys a local cluster with 5 proxies and 5 targets. Examples:

```console
# Deploy 7 targets and 1 proxy:
$ clean_deploy.sh --proxy-cnt 1 --target-cnt 7

# Same as above, plus built-in support for GCP (cloud storage):
$ clean_deploy.sh --proxy-cnt 1 --target-cnt 7 --gcp
```

For more options and detailed description, run `make help` and see: [`clean_deploy.sh`](/docs/development.md#clean-deploy).

### Local Playground Demo

This [video](https://www.youtube.com/watch?v=ANshjHphqfI "AIStore Developer Playground (Youtube video)") gives a quick intro to AIStore, along with a brief demo of the local playground and development environment.

{% include youtubePlayer.html id="ANshjHphqfI" %}

### Manual Deployment

You can also run `make deploy` in the root directory of the repository to deploy a cluster:
```console
$ make deploy
Enter number of storage targets:
10
Enter number of proxies (gateways):
3
Number of local cache directories (enter 0 to use preconfigured filesystems):
2
Select backend providers:
Amazon S3: (y/n) ?
n
Google Cloud Storage: (y/n) ?
n
Azure: (y/n) ?
n
HDFS: (y/n) ?
n
Would you like to create loopback mount points: (y/n) ?
n
Building aisnode: version=df24df77 providers=
```
> Notice the "Cloud" prompt above, and the fact that access to 3rd party Cloud storage is a deployment-time option.

`make kill` will terminate local AIStore if it's already running.

For more development options and tools, please refer to the [development docs](/docs/development.md).

### Testing your cluster

For development, health-checking a new deployment, or for any other (functional and performance testing) related reason you can run any/all of the included tests.

For example:

```console
$ go test ./ais/tests -v -run=Mirror
```

The `go test` above will create an AIS bucket, configure it as a two-way mirror, generate thousands of random objects, read them all several times, and then destroy the replicas and eventually the bucket as well.

Alternatively, if you happen to have Amazon and/or Google Cloud account, make sure to specify the corresponding (S3 or GCS) bucket name when running `go test` commands.
For example, the following will download objects from your (presumably) S3 bucket and distribute them across AIStore:

```console
$ BUCKET=aws://myS3bucket go test ./ais/tests -v -run=download
```

## Kubernetes Playground

In our development and testing, we make use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and the capability, further documented [here](/deploy/dev/k8s/README.md), to run Kubernetes cluster on a single development machine. There's a distinct advantage that AIStore extensions that require Kubernetes - such as [Extract-Transform-Load](etl.md), for example - can be developed rather efficiently.

* [AIStore on Minikube](/deploy/dev/k8s/README.md)

## HTTPS

In the end, all examples above run a bunch of local web servers that listen for plain HTTP requests. Following are quick steps for developers to engage HTTPS:

1. Generate X.509 certificate:

```console
$ openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 1080 -nodes -subj '/CN=localhost'
```

2. Deploy cluster (4 targets, 1 gateway, 6 mountpaths, Google Cloud):

```console
$ AIS_USE_HTTPS=true AIS_SKIP_VERIFY_CRT=true make kill deploy <<< $'4\n1\n6\nn\ny\nn\nn\nn\n'
```

3. Run tests (both examples below list the names of buckets accessible for you in Google Cloud):

```console
$ AIS_ENDPOINT=https://localhost:8080 AIS_SKIP_VERIFY_CRT=true BUCKET=gs://myGCPbucket go test -v -p 1 -count 1 ./ais/tests -run=ListBuckets

$ AIS_ENDPOINT=https://localhost:8080 AIS_SKIP_VERIFY_CRT=true BUCKET=tmp go test -v -p 1 -count 1 ./ais/tests -run=ListBuckets
```

> Notice environment variables above: **AIS_USE_HTTPS**, **AIS_ENDPOINT**, and **AIS_SKIP_VERIFY_CRT**.

## Build, Make and Development Tools

As noted, the project utilizes GNU `make` to build and run things both locally and remotely (e.g., when deploying AIStore via [Kubernetes](/deploy/dev/k8s/Dockerfile). As the very first step, run `make help` for help on:

* **building** AIS binary (called `aisnode`) deployable as both a storage target **or** a proxy/gateway;
* **building** [CLI](/docs/cli.md), [aisfs](/docs/aisfs.md), and benchmark binaries;

In particular, the `make` provides a growing number of developer-friendly commands to:

* **deploy** AIS cluster on your local development machine;
* **run** all or selected tests;
* **instrument** AIS binary with race detection, CPU and/or memory profiling, and more.

## Containerized Deployments: Host Resource Sharing

The following **applies to all containerized deployments**:

1. AIS nodes always automatically detect *containerization*.
2. If deployed as a container, each AIS node independently discovers whether its own container's memory and/or CPU resources are restricted.
3. Finally, the node then abides by those restrictions.

To that end, each AIS node at startup loads and parses [cgroup](https://www.kernel.org/doc/Documentation/cgroup-v2.txt) settings for the container and, if the number of CPUs is restricted, adjusts the number of allocated system threads for its goroutines.

> This adjustment is accomplished via the Go runtime [GOMAXPROCS variable](https://golang.org/pkg/runtime/). For in-depth information on CPU bandwidth control and scheduling in a multi-container environment, please refer to the [CFS Bandwidth Control](https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt) document.

Further, given the container's cgroup/memory limitation, each AIS node adjusts the amount of memory available for itself.

> Limits on memory may affect [dSort](/docs/dsort.md) performance forcing it to "spill" the content associated with in-progress resharding into local drives. The same is true for erasure-coding that also requires memory to rebuild objects from slices, etc.

> For technical details on AIS memory management, please see [this readme](/memsys/README.md).
