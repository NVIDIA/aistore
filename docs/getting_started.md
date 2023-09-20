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

For datasets, say, below 50TB a single host may suffice and should, therefore, be considered a viable option. On the other hand, the [Cloud deployment](#cloud-deployment) option may sound attractive for its ubiquitous convenience and for  _not_ thinking about the hardware and the sizes - at least, not right away.

> Note as well that **you can always start small**: a single-host deployment, a 3-node cluster in the Cloud or on-premises, etc. AIStore supports many options to inter-connect existing clusters - the capability called *unified global namespace* - or migrate existing datasets (on-demand or via supported storage services). For introductions and further pointers, please refer to the [AIStore Overview](/docs/overview.md).

## Prerequisites

AIStore runs on commodity Linux machines with no special requirements whatsoever. It is expected that within a given cluster, all AIS [targets](/docs/overview.md#key-concepts-and-diagrams) are identical, hardware-wise.

* [Linux](#Linux) (with `GCC`, `sysstat` and `attr` packages, and kernel 4.15+) or [macOS](#macOS)
* [Go 1.20 or later](https://golang.org/dl/)
* Extended attributes (`xattrs` - see next section)
* Optionally, Amazon (AWS), Google Cloud Platform (GCP), and/or Azure Cloud Storage accounts.

> See also: `CROSS_COMPILE` comment below.

### Linux

Depending on your Linux distribution, you may or may not have `GCC`, `sysstat`, and/or `attr` packages. These packages must be installed.

Speaking of distributions, our current default recommendation is Ubuntu Server 20.04 LTS. But Ubuntu 18.04 and CentOS 8.x (or later) will also work. As well as numerous others.

For the [local filesystem](/docs/performance.md), we currently recommend xfs. But again, this (default) recommendation shall not be interpreted as a limitation of any kind: other fine choices include zfs, ext4, f2fs, and more.

Since AIS itself provides n-way mirroring and erasure coding, hardware RAID would _not_ be recommended. But can be used, and will work.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or `xattrs`, is a long-time POSIX legacy supported by all mainstream filesystems with no exceptions. Unfortunately, `xattrs` may not always be enabled in the Linux kernel configurations - the fact that can be easily found out by running the `setfattr` command.

If disabled, please make sure to enable xattrs in your Linux kernel configuration. To quickly check:

```console
$ touch foo
$ setfattr -n user.bar -v ttt foo
$ getfattr -n user.bar foo
```

### macOS

macOS/Darwin is also supported, albeit for development only.
Certain capabilities related to querying the state and status of local hardware resources (memory, CPU, disks) may be missing, which is why we **strongly** recommend Linux for production deployments.

The rest of this document is structured as follows:

------------------------------------------------

## Table of Contents

- [Local Playground](#local-playground)
  - [From source](#from-source)
  - [Demo](#demo)
  - [Running Local Playground remotely](#running-local-playground-remotely)
- [Make](#make)
- [System environment variables](#system-environment-variables)
- [Multiple deployment options](#multiple-deployment-options)
  - [Kubernetes deployments](#kubernetes-deployments)
  - [Minimal all-in-one-docker Deployment](#minimal-all-in-one-docker-deployment)
  - [Local Playground Demo](#local-playground-demo)
  - [Manual deployment](#manual-deployment)
  - [Testing your cluster](#testing-your-cluster)
- [Kubernetes Playground](#kubernetes-playground)
- [HTTPS from scratch](#https-from-scratch)
- [Build, Make and Development Tools](#build-make-and-development-tools)
- [Containerized Deployments: Host Resource Sharing](#containerized-deployments-host-resource-sharing)

## Local Playground

If you're looking for a speedy evaluation, feature experimentation, initial usage, or development, running AIS from its GitHub source might be a good option.

Hence, **Local Playground** - one of the several supported [deployment options](#multiple-deployment-options).

> Local Playground is **not intended** for production and is not meant to provide optimal performance.

To run AIStore from source, you'd typically need **Go**: compiler, linker, tools, and required packages. However:

> `CROSS_COMPILE` option (see below) can be used to build AIStore without having (to install) [Go](https://golang.org/dl/) and its toolchain (requires Docker).

To install Go(lang) on Linux:

* Download the latest `go1.20.<x>.linux-amd64.tar.gz` from [Go downloads](https://golang.org/dl/)
* Follow [installation instructions](https://go.dev/doc/install)
* **Or** simply run: `tar -C /usr/local -xzf go1.20.<x>.linux-amd64.tar.gz` and add `/usr/local/go/bin` to $PATH

Next, if not done yet, export the [`GOPATH`](https://go.dev/doc/gopath_code#GOPATH) environment variable.

Here's an additional [5-minute introduction](/deploy/dev/local/README.md) that talks about setting up the Go environment and also includes:

* provisioning data drives for AIS deployment, and
* running a single-node AIS cluster locally.

Once done, run AIS as follows:

### From source

The steps:

```console
$ cd $GOPATH/src/github.com/NVIDIA
$ git clone https://github.com/NVIDIA/aistore.git
$ cd aistore
# Optionally, run `make mod-tidy` to preload dependencies
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

For more options and detailed descriptions, run `make help` and see: [`clean_deploy.sh`](/docs/development.md#clean-deploy).

### Demo

Here's a quick, albeit somewhat outdated, [YouTube introduction and demo](https://www.youtube.com/watch?v=ANshjHphqfI).

### Running Local Playground remotely

AIStore (product and solution) is fully based on HTTP(S) utilizing the protocol both externally (to support both frontend interfaces and communications with remote backends) and internally, for [intra-cluster streaming](/transport).

Connectivity-wise, what that means is that your local deployment at `localhost:8080` can as easily run at any **arbitrary HTTP(S)** address.

Here're the quick change you make to deploy Local Playground at (e.g.) `10.0.0.207`, whereby the main gateway's listening port would still remain `8080` default:

```diff
diff --git a/deploy/dev/local/aisnode_config.sh b/deploy/dev/local/aisnode_config.sh                                                             |
index 9198c0de4..be63f50d0 100755                                                                                                                |
--- a/deploy/dev/local/aisnode_config.sh                                                                                                         |
+++ b/deploy/dev/local/aisnode_config.sh                                                                                                         |
@@ -181,7 +181,7 @@ cat > $AIS_LOCAL_CONF_FILE <<EOL                                                                                             |
        "confdir": "${AIS_CONF_DIR:-/etc/ais/}",                                                                                                 |
        "log_dir":       "${AIS_LOG_DIR:-/tmp/ais$NEXT_TIER/log}",                                                                               |
        "host_net": {                                                                                                                            |
-               "hostname":                 "${HOSTNAME_LIST}",                                                                                  |
+               "hostname":                 "10.0.0.207",                                                                                        |
                "hostname_intra_control":   "${HOSTNAME_LIST_INTRA_CONTROL}",                                                                    |
                "hostname_intra_data":      "${HOSTNAME_LIST_INTRA_DATA}",                                                                       |
                "port":               "${PORT:-8080}",                                                                                           |
diff --git a/deploy/dev/local/deploy.sh b/deploy/dev/local/deploy.sh                                                                             |
index e0b467d82..b18361155 100755                                                                                                                |
--- a/deploy/dev/local/deploy.sh                                                                                                                 |
+++ b/deploy/dev/local/deploy.sh                                                                                                                 |
@@ -68,7 +68,7 @@ else                                                                                                                           |
   PORT_INTRA_DATA=${PORT_INTRA_DATA:-13080}                                                                                                     |
   NEXT_TIER="_next"                                                                                                                             |
 fi                                                                                                                                              |
-AIS_PRIMARY_URL="http://localhost:$PORT"                                                                                                        |
+AIS_PRIMARY_URL="http://10.0.0.207:$PORT"                                                                                                       |
 if $AIS_USE_HTTPS; then                                                                                                                         |
   AIS_PRIMARY_URL="https://localhost:$PORT"                                                                                                     |
```

## Make

AIS comes with its own build system that we use to build both standalone binaries and container images for a variety of deployment options.

The very first `make` command you may want to execute could as well be:

```console
$ make help
```

This shows all subcommands, environment variables, and numerous usage examples, including:

```console
Examples:
# Deploy cluster locally
$ make deploy

# Stop locally deployed cluster and cleanup all cluster-related data and bucket metadata (but not cluster map)
$ make kill clean

# Stop and then deploy (non-interactively) cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build `aisnode` executable with the support for GCP and AWS backends
$ make kill deploy <<< $'7\n2\n4\ny\ny\nn\nn\n0\n'
```

## System environment variables

The variables include `AIS_ENDPOINT`, `AIS_AUTHN_TOKEN_FILE`, and [more](/api/env).

Almost in all cases, there's an "AIS_" prefix (hint: `git grep AIS_`).

And in all cases with no exception, the variable takes precedence over the corresponding configuration, if exists. For instance:

```console
AIS_ENDPOINT=https://10.0.1.138 ais show cluster
```

overrides the default endpoint as per `ais config cli` or (same) `ais config cli --json`

> Endpoints are equally provided by each and every running AIS gateway (aka "proxy") and each endpoint can be (equally) used to access the cluster. To find out what's currently configured, run (e.g.):

```console
$ ais config node <NODE> local host_net --json
```

where `NODE` is, effectively, any clustered proxy (that'll show up if you type `ais config node` and press `<TAB-TAB>`).

Other variables, such as `AIS_IS_PRIMARY` and `AIS_USE_HTTPS` can prove to be useful at deployment time for the most part.

For developers, CLI `ais config cluster log.modules ec xs` (for instance) would allow to selectively raise and/or reduce logging verbosity on a per module bases - modules EC (erasure coding) and xactions (batch jobs) in this particular case.

> To list all log modules, type `ais config cluster` or `ais config node` and press `<TAB-TAB>`.

## Multiple deployment options

All [containerized deployments](/deploy/README.md) have their own separate `Makefiles`. With the exception of [local playground](#local-playground), each specific build-able development (`dev/`) and production (`prod/`) option under the `deploy` folder has a pair: {`Dockerfile`, `Makefile`}.

> This separation is typically small in size and easily readable and maintainable.

Also supported is the option *not* to have the [required](#prerequisites) [Go](https://go.dev) installed and configured. To still be able to build AIS binaries without [Go](https://go.dev) on your machine, make sure that you have `docker` and simply uncomment `CROSS_COMPILE` line in the top [`Makefile`](./../Makefile).

AIStore deploys anywhere anytime supporting multiple deployment options [summarized and further referenced here](/deploy/README.md).

In particular:

### Kubernetes deployments

For any Kubernetes deployments (including, of course, production deployments) please use a separate and dedicated [AIS-K8s GitHub](https://github.com/NVIDIA/ais-k8s/blob/master/docs/README.md) repository. The repo contains [Helm Charts](https://github.com/NVIDIA/ais-k8s/tree/master/helm/ais/charts) and detailed [Playbooks](https://github.com/NVIDIA/ais-k8s/tree/master/playbooks) that cover a variety of use cases and configurations.

In particular, [AIS-K8s GitHub repository](https://github.com/NVIDIA/ais-k8s/blob/master/terraform/README.md) provides a single-line command to deploy Kubernetes cluster and the underlying infrastructure with the AIStore cluster running inside (see below). The only requirement is having a few dependencies preinstalled (in particular, `helm`) and a Cloud account.

The following GIF illustrates the steps to deploy AIS on the Google Cloud Platform (GCP):

![Kubernetes cloud deployment](images/ais-k8s-deploy.gif)

Finally, the [repository](https://github.com/NVIDIA/ais-k8s) hosts the [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/tree/master/operator) project that will eventually replace Helm charts and will become the main deployment, lifecycle, and operation management "vehicle" for AIStore.

### Minimal all-in-one-docker Deployment

This option has the unmatched convenience of requiring an absolute minimum time and resources - please see this [README](/deploy/prod/docker/single/README.md) for details.

### Manual deployment

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
Create loopback devices (note that it may take some time): (y/n) ?
n
Building aisnode: version=df24df77 providers=
```
> Notice the "Cloud" prompt above and the fact that access to 3rd party Cloud storage is a deployment-time option.

Run `make help` for supported (make) options and usage examples, including:

```console
# Restart a cluster of 7 targets (4 mountpaths each) and 2 proxies; utilize previously generated (pre-shutdown) local configurations
$ make restart <<< $'7\n2\n4\ny\ny\nn\nn\n0\n'

# Redeploy the cluster (4 targets, 1 proxyi, 4 mountoaths); build `aisnode` executable for debug without any backend-supporting libraries; use RUN_ARGS to pass an additional command-line option ('-override_backends=true') to each running node
$ RUN_ARGS=-override_backends MODE=debug make kill deploy <<< $'4\n1\n4\nn\nn\nn\nn\n0\n'

# Same as above, but additionally run all 4 targets in a standby mode
$ RUN_ARGS='-override_backends -standby' MODE=debug make kill deploy <<< $'4\n1\n4\nn\nn\nn\nn\n0\n'
...
...
```

Further:

* `make kill`    - terminate local AIStore.
* `make restart` - shut it down and immediately restart using the existing configuration.
* `make help`    - show make options and usage examples.

For even more development options and tools, please refer to:

* [development docs](/docs/development.md)

### Testing your cluster

For development, health-checking a new deployment, or for any other (functional and performance testing) related reason you can run any/all of the included tests.

For example:

```console
$ go test ./ais/test -v -run=Mirror
```

The `go test` above will create an AIS bucket, configure it as a two-way mirror, generate thousands of random objects, read them all several times, and then destroy the replicas and eventually the bucket as well.

Alternatively, if you happen to have Amazon and/or Google Cloud account, make sure to specify the corresponding (S3 or GCS) bucket name when running `go test` commands.
For example, the following will download objects from your (presumably) S3 bucket and distribute them across AIStore:

```console
$ BUCKET=aws://myS3bucket go test ./ais/test -v -run=download
```

To run all tests in the category [short tests](https://pkg.go.dev/testing#Short):

```console
# using randomly named ais://nnn bucket (that will be created on the fly and destroyed in the end):
$ BUCKET=ais://nnn make test-short

# with existing Google Cloud bucket gs://myGCPbucket
$ BUCKET=gs://myGCPbucket make test-short
```

The command randomly shuffles existing short tests and then, depending on your platform, usually takes anywhere between 15 and 30 minutes. To terminate, press Ctrl-C at any time.

> Ctrl-C or any other (kind of) abnormal termination of a running test may have a side effect of leaving some test data in the test bucket.

## Kubernetes Playground

In our development and testing, we make use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and the capability, further documented [here](/deploy/dev/k8s/README.md), to run the Kubernetes cluster on a single development machine. There's a distinct advantage that AIStore extensions that require Kubernetes - such as [Extract-Transform-Load](/docs/etl.md), for example - can be developed rather efficiently.

* [AIStore on Minikube](/deploy/dev/k8s/README.md)

## HTTPS from scratch

In the end, all examples above run a bunch of local web servers that listen for plain HTTP requests. Following are quick steps for developers to engage HTTPS.

This is still a so-called _local playground_ type deployment _from scratch_, whereby we are not trying to switch an existing cluster from HTTP to HTTPS, or vice versa. All we do here is deploying a brand new HTTPS-based aistore.

**1**. Generate X.509 certificate:

```console
$ openssl req -x509 -newkey rsa:4096 -keyout server.key -out server.crt -days 1080 -nodes -subj '/CN=localhost'
```

**2**. Deploy cluster (4 targets, 1 gateway, 6 mountpaths, Google Cloud):

```console
$ AIS_USE_HTTPS=true AIS_SKIP_VERIFY_CRT=true make kill deploy <<< $'4\n1\n6\nn\ny\nn\nn\n0\n'
```

**3**. Run tests (both examples below list the names of buckets accessible for you in Google Cloud):

```console
$ AIS_ENDPOINT=https://localhost:8080 AIS_SKIP_VERIFY_CRT=true BUCKET=gs://myGCPbucket go test -v -p 1 -count 1 ./ais/test -run=ListBuckets

$ AIS_ENDPOINT=https://localhost:8080 AIS_SKIP_VERIFY_CRT=true BUCKET=tmp go test -v -p 1 -count 1 ./ais/test -run=ListBuckets
```

**4**. To use CLI, try first any command with HTTPS-based cluster endpoint, for instance:

```console
$ AIS_ENDPOINT=https://127.0.0.1:8080 ais show cluster
```

But if it fails with "failed to verify certificate" message, perform a simple step to configufre CLI to skip HTTPS cert validation:

```console
$ ais config cli set cluster.skip_verify_crt true
"cluster.skip_verify_crt" set to: "true" (was: "false")
```

And then try again.

> Notice environment variables above: **AIS_USE_HTTPS**, **AIS_ENDPOINT**, and **AIS_SKIP_VERIFY_CRT**.

> See also: [switching an already deployed cluster between HTTP and HTTPS](/docs/switch_https.md)

## Build, Make and Development Tools

As noted, the project utilizes GNU `make` to build and run things both locally and remotely (e.g., when deploying AIStore via [Kubernetes](/deploy/dev/k8s/Dockerfile). As the very first step, run `make help` for help on:

* **building** AIS node binary (called `aisnode`) deployable as both storage target **or** an ais gateway (most of the time referred to as "proxy");
* **building** [CLI](/docs/cli.md)
* **building** [benchmark tools](/bench/tools/README.md).

In particular, the `make` provides a growing number of developer-friendly commands to:

* **deploy** the AIS cluster on your local development machine;
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

> Memory limits may affect [dSort](/docs/dsort.md) performance forcing it to "spill" the content associated with in-progress resharding into local drives. The same is true for erasure-coding which also requires memory to rebuild objects from slices, etc.

> For technical details on AIS memory management, please see [this readme](/memsys/README.md).
