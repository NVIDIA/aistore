# Getting Started

You could start running AIS on a single Linux or Mac host. Alternatively, or in addition, you could quickly deploy your first AIS cluster on [GCP/GKE](https://cloud.google.com/kubernetes-engine). This document introduces these and other supported options and provides quick links for further information.

Generally, when deciding how to deploy a system like AIS with so many options to choose from, a good place to start would be answering the following two fundamental questions:

* what's the dataset size, or sizes?
* what hardware will I use?

For datasets, say, below 50TB a single host may suffice and should, therefore, be considered a viable option. On the other hand, [Cloud deployment](#cloud-deployment) option may sound attractive for its ubiquitous convenience and for  _not_ thinking about the hardwares and the sizes - at least, not right away.

> Note as well that **you can always start small**: a single-host deployment, a 3-node cluster in the Cloud or on premises, etc. AIStore supports a number of options to inter-connect existing clusters - the capability called *unified global namespace* - or migrate existing datasets (on demand or via supported storage services). For introductions and further pointers, please refer to the [AIStore Overview](overview.md).

## Prerequisites

AIStore runs on commodity Linux machines with no special hardware requirements whatsoever.

> It is expected that within a given cluster, all AIS target machines are identical, hardware-wise.

* [Linux](#Linux) (with `gcc`, `sysstat` and `attr` packages, and kernel 4.15+) or [MacOS](#MacOS)
* [Go 1.15 or later](https://golang.org/dl/)
* Extended attributes (`xattrs` - see below)
* Optionally, Amazon (AWS) or Google Cloud Platform (GCP) account(s)

### Linux host

Depending on your Linux distribution, you may or may not have `gcc`, `sysstat`, and/or `attr` packages.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or xattrs, is a long time POSIX legacy and is supported by all mainstream filesystems with no exceptions.
Unfortunately, extended attributes (xattrs) may not always be enabled (by the Linux distribution you are using) in the Linux kernel configurations - the fact that can be easily found out by running `setfattr` command.

> If disabled, please make sure to enable xattrs in your Linux kernel configuration.

### MacOS host

MacOS/Darwin is also supported, albeit for development only.
Certain capabilities related to querying the state-and-status of local hardware resources (memory, CPU, disks) may be missing, which is why we **strongly** recommend Linux for production deployments.

## Cloud Deployment

[AIS-K8s GitHub repository](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md) provides the (single-line) command to deploy Kubernetes cluster and the underlying infrastructure with AIStore cluster running inside. The only requirement is having a few dependencies installed (e.g., `helm`) and a Cloud account.

The following GIF presents a flow of this deployment.
  
<img src="/docs/images/ais-k8s-deploy.gif" alt="Kubernetes cloud deployment" width="100%">

> If you already have a Kubernetes cluster deployed in the cloud, and you need more customization, refer to [Kubernetes production deployment](#kubernetes-production-deployment).

## Production Deployment: Kubernetes

This type of deployment is described in [AIS-K8s GitHub repo](https://github.com/NVIDIA/ais-k8s/blob/master/docs/README.md) containing all necessary information.
It includes Helm Charts and dedicated documentation for different use cases and configurations.

## Minimal all-in-one-docker Deployment

This option has the unmatched convenience of requiring an absolute minimum time and resources - please see this [README](/deploy/prod/docker/single/README.md) for details.

## Local Playground

You could use the instruction below for a quick evaluation, experimenting with features, first-time usage - and, of course, for development.
Local AIStore playground is not intended for production clusters and is not meant to provide optimal performance.

The following [video](https://www.youtube.com/watch?v=ANshjHphqfI "AIStore Developer Playground (Youtube vide)") gives a quick intro to AIStore, along with a brief demo of the local playground and development environment.

[<img src="images/dev-playground-400.png" alt="AIStore Developer Playground video" width="400">](https://www.youtube.com/watch?v=ANshjHphqfI "AIStore Developer Playground (Youtube video)")

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

## Kubernetes Playground

In our development and testing, we make use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and the capability, further documented [here](/deploy/dev/k8s/README.md), to run Kubernetes cluster on a single development machine. There's a distinct advantage that AIStore extensions that require Kubernetes - such as [Extract-Transform-Load](/docs/etl.md), for example - can be developed rather efficiently.

* [AIStore on Minikube](/deploy/dev/k8s/README.md)

## HTTPS

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

As noted, the project utilizes GNU `make` to build and run things both locally and remotely (e.g., when deploying AIStore via [Kubernetes](/deploy/dev/k8s/Dockerfile). As the very first step, run `make help` for help on:

* **building** AIS binary (called `aisnode`) deployable as both a storage target **or** a proxy/gateway;
* **building** [CLI](/cmd/cli/README.md), [aisfs](/cmd/aisfs/README.md), and benchmark binaries;

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

> Limits on memory may affect [dSort](/dsort/README.md) performance forcing it to "spill" the content associated with in-progress resharding into local drives. The same is true for erasure-coding that also requires memory to rebuild objects from slices, etc.

> For technical details on AIS memory management, please see [this readme](/memsys/README.md).

