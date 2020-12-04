# Getting Started with AIStore

This README provides information and references to documentation about deployment.
Deployment options are practically unlimited and include a spectrum with bare-metal (Kubernetes) clusters of any size, on the one hand, and a single Linux or Mac host, on the other.

It is divided into two sections:
- [On cloud deployment - AIStore on cloud provider hardware](#on-cloud-deployment).
This section contains information on deploying the infrastructure and AIStore without own hardware, only with a cloud provider account.
- [On-premise deployment - AIStore on own hardware](#on-premise-deployment).
This section contains all information required to deploy AIStore on own hardware, including production, development, or playground deployment.

## On cloud deployment

[AIS-K8s GitHub repository](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md) provides the single-line command to deploy Kubernetes cluster (and the underlying infrastructure) with AIStore cluster running inside.
This deployment's only requirement is having a few dependencies installed (like `helm`) and a working cloud provided account.

The following GIF presents a flow of this deployment.
  
<img src="/docs/images/ais-k8s-deploy.gif" alt="Kubernetes cloud deployment" width="100%">

> If you already have a Kubernetes cluster deployed in the cloud, and you need more customization, refer to [Kubernetes production deployment](#kubernetes-production-deployment).

## On-premise deployment

This section is divided into two parts:
- [Prerequisites](#prerequisites) - describes requirements which have to be met when deploying AIStore on own hardware;
- [Deployments](#deployments) - focuses on different ways to deploy the AIStore on own hardware.
    - [Kubernetes production deployment](#kubernetes-production-deployment)
    - [Local production deployment](#local-production-deployment)
    - [Local playground and development deployment](#local-playground-and-development-deployment)
    - [Local Docker deployment](#local-docker-deployment)
    - [Local Kubernetes deployment](#local-kubernetes-deployment)

### Prerequisites

AIStore runs on commodity Linux machines with no special hardware requirements whatsoever.

> It is expected that within a given cluster, all AIS target machines are identical, hardware-wise.

* [Linux](#Linux) (with `gcc`, `sysstat` and `attr` packages, and kernel 4.15+) or [MacOS](#MacOS)
* [Go 1.15 or later](https://golang.org/dl/)
* Extended attributes (`xattrs` - see below)
* Optionally, Amazon (AWS) or Google Cloud Platform (GCP) account(s)

#### Linux host

Depending on your Linux distribution, you may or may not have `gcc`, `sysstat`, and/or `attr` packages.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or xattrs, is a long time POSIX legacy and is supported by all mainstream filesystems with no exceptions.
Unfortunately, extended attributes (xattrs) may not always be enabled (by the Linux distribution you are using) in the Linux kernel configurations - the fact that can be easily found out by running `setfattr` command.

> If disabled, please make sure to enable xattrs in your Linux kernel configuration.

#### MacOS host

MacOS/Darwin is also supported, albeit for development only.
Certain capabilities related to querying the state-and-status of local hardware resources (memory, CPU, disks) may be missing, which is why we **strongly** recommend Linux for production deployments.

#### Containerized Deployments: Host Resource Sharing

The following **applies to all containerized deployments**:

1. AIS nodes always automatically detect *containerization*.
2. If deployed as a container, each AIS node independently discovers whether its own container's memory and/or CPU resources are restricted.
3. Finally, the node then abides by those restrictions.

To that end, each AIS node at startup loads and parses [cgroup](https://www.kernel.org/doc/Documentation/cgroup-v2.txt) settings for the container and, if the number of CPUs is restricted, adjusts the number of allocated system threads for its goroutines.

> This adjustment is accomplished via the Go runtime [GOMAXPROCS variable](https://golang.org/pkg/runtime/). For in-depth information on CPU bandwidth control and scheduling in a multi-container environment, please refer to the [CFS Bandwidth Control](https://www.kernel.org/doc/Documentation/scheduler/sched-bwc.txt) document.

Further, given the container's cgroup/memory limitation, each AIS node adjusts the amount of memory available for itself.

> Limits on memory may affect [dSort](dsort/README.md) performance forcing it to "spill" the content associated with in-progress resharding into local drives. The same is true for erasure-coding that also requires memory to rebuild objects from slices, etc.

> For technical details on AIS memory management, please see [this readme](memsys/README.md).

### Deployments

#### Kubernetes production deployment

This type of deployment is described in [AIS-K8s GitHub repo](https://github.com/NVIDIA/ais-k8s/blob/master/docs/README.md) containing all necessary information.
It includes Helm Charts and dedicated documentation for different use cases and configurations.

#### Local production deployment

For production deployment on a local machine, please refer to this [README](/deploy/prod/docker/single/README.md).

#### Local playground and development deployment

This type of deployment is meant for a first-time usage, experimenting with AIStore features, or development.
However, it is not meant to provide the best performance. 
For this purpose, other deployment types should be used.

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

##### HTTPS

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

##### Build, Make and Development Tools

As noted, the project utilizes GNU `make` to build and run things both locally and remotely (e.g., when deploying AIStore via [Kubernetes](deploy/dev/k8s/Dockerfile). As the very first step, run `make help` for help on:

* **building** AIS binary (called `aisnode`) deployable as both a storage target **or** a proxy/gateway;
* **building** [CLI](cmd/cli/README.md), [aisfs](cmd/aisfs/README.md), and benchmark binaries;

In particular, the `make` provides a growing number of developer-friendly commands to:

* **deploy** AIS cluster on your local development machine;
* **run** all or selected tests;
* **instrument** AIS binary with race detection, CPU and/or memory profiling, and more.

#### Local Docker deployment

[Local Playground](#local-playground-and-development-deployment) is probably the speediest option to run AIS clusters.
However, to take advantage of containerization (which includes, for instance, multiple logically-isolated configurable networks), you can also run AIStore as described here:

* [Getting started with Docker](docs/docker_main.md).

<!-- videoStart
{% include_relative docker_videos.md %}
videoEnd -->

#### Local Kubernetes deployment

This deployment option makes use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and is documented [here](/deploy/dev/k8s/README.md).
Its most significant advantages are checking the AIStore behavior on the Kubernetes and the ability to run [Extract-Transform-Load](/docs/etl.md) operations on the data stored in the cluster.
