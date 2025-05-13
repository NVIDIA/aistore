---
layout: post
title: GETTING STARTED
permalink: /docs/getting-started
redirect_from:
 - /getting_started.md/
 - /docs/getting_started.md/
---

AIStore can scale from a **single Linux machine** to a **rack-scale cluster** or a managed Kubernetes installation in the cloud.

## Deployment Considerations

Before you pick a path, answer two quick questions:

1. **Dataset size** – e.g., tens of terabytes or multi-petabyte?
2. **Available hardware** – laptop, workstation, a few bare-metal servers, or Kubernetes?

For datasets, say, below 50TB a single host may suffice and should, therefore, be considered a viable option. Expecting growth or already past that mark? Plan for multi-node or cloud.

Note as well that you can always start small: a single-host deployment, a 3-node cluster in the Cloud or on-premises, etc. AIStore supports many options to inter-connect existing clusters - the capability called [unified namespace](/docs/overview.md#unified-namespace) - or migrate existing datasets (on-demand or via supported storage services). For introductions and further pointers, please refer to the [AIStore Overview](/docs/overview.md).

## Prerequisites

AIStore runs on commodity Linux machines with no special requirements whatsoever. It is expected that within a given cluster, all AIS [targets](/docs/overview.md#target) are identical, hardware-wise.

* Linux with `gcc`, `sysstat`, `attr`, `util-linux`
* Linux **kernel ≥ 6.8**
* [Go ≥ 1.23](https://golang.org/dl/) (or build via `CROSS_COMPILE`)
* Local filesystem with **extended attributes** ([xattrs](https://en.wikipedia.org/wiki/Extended_file_attributes)) enabled
* *Optional* – cloud credentials (AWS, GCP, Azure, OCI)
* *Optional* – [golangci-lint 2.x](https://golangci-lint.run/welcome/install/)

[Mac](#mac) is also supported albeit in a limited (development only) way.

### Linux

Depending on your Linux distribution, you may or may not have `GCC`, `sysstat`, and/or `attr` packages. These packages must be installed.

Speaking of distributions, our current default recommendation is Ubuntu Server 24.04 LTS or Ubuntu Server 22.04 LTS, as it has been our experience. However, AIStore has no special dependencies, so virtually any distribution will work.

For the [local filesystem](/docs/performance.md), we currently recommend xfs. But again, this (default) recommendation shall not be interpreted as a limitation of any kind: other fine choices include zfs, ext4, f2fs and more.

Since AIS itself provides n-way mirroring and erasure coding, hardware RAID would not be recommended. But can be used, and will work.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or `xattrs`, is a long-time POSIX legacy supported by all mainstream filesystems with no exceptions. Unfortunately, `xattrs` may not always be enabled in the Linux kernel configurations - the fact that can be easily found out by running the `setfattr` command.

If disabled, please make sure to enable xattrs in your Linux kernel configuration. To quickly check:

```console
$ touch foo
$ setfattr -n user.bar -v ttt foo
$ getfattr -n user.bar foo
```

### Mac

For developers, there's also macOS aka Darwin option. Certain capabilities related to querying the state and status of local hardware resources (memory, CPU, disks) may be missing. In fact, it is easy to review specifics with a quick check on the sources:

```console
$ find . -name "*darwin*"

./fs/fs_darwin.go
./cmn/cos/err_darwin.go
./sys/proc_darwin.go
./sys/cpu_darwin.go
./sys/mem_darwin.go
./ios/diskstats_darwin.go
./ios/dutils_darwin.go
./ios/fsutils_darwin.go
...
```

Benchmarking and stress-testing is also being done on Linux only - another reason to consider Linux (and only Linux) for production deployments.

The rest of this document is structured as follows:

------------------------------------------------

## Document Structure

The rest of this document is structured as follows:

- [Finding Things (Tip)](#finding-things-tip)
- [Local Playground](#local-playground)
- [Make](#make)
- [System environment variables](#system-environment-variables)
- [Multiple deployment options](#multiple-deployment-options)
- [Running AIStore in Google Colab](#running-aistore-in-google-colab)
- [Kubernetes Playground](#kubernetes-playground)
- [Setting Up HTTPS Locally](#setting-up-https-locally)
- [Build, Make, and Development Tools](#build-make-and-development-tools)
- [Containerized Deployments: Host Resource Sharing](#containerized-deployments-host-resource-sharing)
- [Assorted Curl](#assorted-curl)

## Finding Things (Tip)

AIStore has been around for a while; the repository has accumulated quite a bit of information that can be immediately located as follows:

1. See [Extended Index](/docs/docs.md)
2. Use CLI `search` command, e.g.: `ais search copy`
3. Clone the repository and run `git grep`, e.g.: `git grep -n out-of-band -- "*.md"`

Any of the above will work. In particular, for any keyword or text of any kind, you can easily look up examples and descriptions via a simple `find` or `git grep` command. For instance:

```console
$ git grep -n out-of-band -- "*.md"
docs/cli/archive.md:555:         - detecting remote version changes (a.k.a. out-of-band updates), and
...
...
$ git grep out-of-band -- "*.md" | wc -l
44
```

Alternatively, use a combination of `find`, `xargs`, and/or `grep` to search through existing texts of any kind, including source comments. For example:

```console
$ find . -name "*.md" | xargs grep -n "out-of-band"
```

In addition, there's the user-friendly [CLI](/docs/cli.md). For example, to search for commands related to copy, you could:

```console
$ ais search copy

ais bucket cp
ais cp
ais download
ais job rm download
ais job start copy-bck
ais job start download
ais job start mirror
ais object cp
ais start copy-bck
ais start download
ais start mirror
...
```

For the CLI, remember to use the `--help` option, which will universally show specific supported options and usage examples. For example:

```console
$ ais cp --help
```

## Local Playground

If you're looking for speedy evaluation, want to experiment with [supported features](https://github.com/NVIDIA/aistore/tree/main?tab=readme-ov-file#features), get a feel of initial usage, or development - for any and all of these reasons running AIS from its GitHub source might be a good option.

Hence, we introduced (and keep maintaining) **Local Playground** - one of the several supported [deployment options](#multiple-deployment-options).

> Some of the most popular deployment options are also **summarized** in this [table](https://github.com/NVIDIA/aistore/tree/main/deploy#readme). The list includes Local Playground, and its complementary guide [here](https://github.com/NVIDIA/aistore/blob/main/deploy/dev/local/README.md).

> Local Playground is **not intended** for production and is not meant to provide optimal performance.

To run AIStore from source, one would typically need to have **Go**: compiler, linker, tools, and required packages. However:

> `CROSS_COMPILE` option (see below) can be used to build AIStore without having (to install) [Go](https://golang.org/dl/) and its toolchain (requires Docker).

To install Go(lang) on Linux:

* Download the latest `go1.<x.y>.linux-amd64.tar.gz` from [Go downloads](https://golang.org/dl/)
* Follow [installation instructions](https://go.dev/doc/install)

Next, if not done yet, export the [`GOPATH`](https://go.dev/doc/gopath_code#GOPATH) environment variable.

> Here's an additional [5-minute introduction](https://github.com/NVIDIA/aistore/blob/main/deploy/dev/local/README.md) that talks more in-depth about setting up the Go environment variables.

Once done, we can run AIS as follows (**steps 1 through 4** below):

#### Step 1: Clone the AIStore repository and preload dependencies

We want to clone the repository into the following path so we can access
some of the associated binaries through the environment variables we set up earlier.

```console
$ cd $GOPATH/src/github.com/NVIDIA
$ git clone https://github.com/NVIDIA/aistore.git
$ cd aistore
```

To preload dependencies, optionally, run `go mod tidy` (or same, `make mod-tidy`):

```console
$ make mod-tidy
```

#### Step 2: Deploy cluster and verify the running status using `ais` cli

> **NOTE**: For a local deployment, we do not need production filesystem paths. For more information, read about [configuration basics](/docs/configuration.md#rest-of-this-document-is-structured-as-follows). If you need a physical disk or virtual block device, you must add them to the fspaths config. See [running local playground with emulated disks](#running-local-playground-with-emulated-disks) for more information.

Many useful commands are provided via top [Makefile](https://github.com/NVIDIA/aistore/blob/main/Makefile) (for details, see [Make](#make) section below).

In particular, we can use `make` to deploy our very first 3 nodes (and 3 gateways) cluster:

```console
$ make kill clean cli aisloader deploy <<< $'3\n3'
```

This `make` command executes several make targets (not to confuse with ais targets) - in particular, it:

* shuts down (via `make kill`) AIStore that _may_ have been previously deployed in the local playground;
* removes its metadata and data (`make clean`);
* builds CLI (`ais`) and `aisloader` tools (that we are using all the time);

and, finally:

* deploys (3 storage nodes, 3 gateways) cluster.

The cluster than can be observed as follows:

```console
$ ais show cluster
```

#### `clean_deploy.sh`

Alternatively or in addition (to `make deploy`), one can also use:

* [`clean_deploy.sh` script](https://github.com/NVIDIA/aistore/blob/main/scripts/clean_deploy.sh)
* [`clean_deploy.sh README`](/docs/development.md#clean-deploy)

With no arguments, this script also builds AIStore binaries (such as `aisnode` and `ais` CLI). You can pass in arguments to configure the same options that the `make deploy` command above uses.

```console
$ ./scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --mountpath-cnt 1 --deployment local --cleanup
```

#### Step 3: Run `aisloader` tool

We can now run the `aisloader` tool to benchmark our new cluster.

```console
$ make aisloader # build aisloader tool

$ aisloader -bucket=ais://abc -duration 2m -numworkers=8 -minsize=1K -maxsize=1K -pctput=100 --cleanup=false # run aisloader for 2 minutes (8 workers, 1KB size, 100% write, no cleanup)
```

#### Step 4: Run iostat (or use any of the multiple [documented](/docs/prometheus.md) ways to monitor AIS performance)

```console
$ iostat -dxm 10 sda sdb
```

### Running Local Playground with emulated disks

Here's a quick walk-through (with more references included below).

* Step 1: patch `deploy/dev/local/aisnode_config.sh` as follows:

```diff
diff --git a/deploy/dev/local/aisnode_config.sh b/deploy/dev/local/aisnode_config.sh
index c5e0e4fae..46085e19c 100755
--- a/deploy/dev/local/aisnode_config.sh
+++ b/deploy/dev/local/aisnode_config.sh
@@ -192,11 +192,12 @@ cat > $AIS_LOCAL_CONF_FILE <<EOL
                "port_intra_data":    "${PORT_INTRA_DATA:-10080}"
        },
        "fspaths": {
-               $AIS_FS_PATHS
+               "/tmp/ais/mp1": "",
+               "/tmp/ais/mp2": ""
        },
        "test_fspaths": {
                "root":     "${TEST_FSPATH_ROOT:-/tmp/ais$NEXT_TIER/}",
-               "count":    ${TEST_FSPATH_COUNT:-0},
+               "count":    0,
                "instance": ${INSTANCE:-0}
        }
 }
```

* Step 2: deploy a single target with two loopback devices (1GB size each):

```console
$ make kill clean cli deploy <<< $'1\n1\n4\ny\ny\nn\n1G\n'
```

or, same:

```console
$ TAGS=aws TEST_LOOPBACK_SIZE=1G make kill clean cli deploy <<< $'1\n1\n'
```


$ mount | grep dev/loop
/dev/loop23 on /tmp/ais/mp1 type ext4 (rw,relatime)
/dev/loop25 on /tmp/ais/mp2 type ext4 (rw,relatime)
```

* Step 3: observe a running cluster; notice the deployment [type](#multiple-deployment-options) and the number of disks:

```console
$ ais show cluster
PROXY            MEM USED(%)     MEM AVAIL       LOAD AVERAGE    UPTIME  STATUS  VERSION         BUILD TIME
p[BOxqibgv][P]   0.14%           27.28GiB        [1.2 1.1 1.1]   -       online  3.22.bf26375e5  2024-02-29T11:11:52-0500

TARGET           MEM USED(%)     MEM AVAIL    CAP USED(%)  CAP AVAIL    LOAD AVERAGE    REBALANCE   UPTIME  STATUS  VERSION
t[IwzSpiIm]      0.14%           27.28GiB     6%           1.770GiB     [1.2 1.1 1.1]   -           -       online  3.22.bf26375e5

Summary:
   Proxies:             1
   Targets:             1 (num disks: 2)
   Cluster Map:         version 4, UUID g7sPH9dTY, primary p[BOxqibgv]
   Deployment:          linux
   Status:              2 online
   Rebalance:           n/a
   Authentication:      disabled
   Version:             3.22.bf26375e5
   Build:               2024-02-29T11:11:52-0500
```

See also:
> [for developers](development.md);
> [cluster and node configuration](configuration.md);
> [supported deployments: summary table and links](https://github.com/NVIDIA/aistore/blob/main/deploy/README.md).

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

### Example: deploy cluster locally
```console
$ make deploy
```

### Example: shutdown cluster and cleanup all its data and metadata
```console
$ make kill clean
```

### Example: shutdown/cleanup, build CLI, and then deploy non-interactively a cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build `aisnode` executable with GCP and AWS backends
```console
$ make kill clean cli deploy <<< $'7\n2\n4\ny\ny\nn\n'
```

### Example: same as above
```console
$ AIS_BACKEND_PROVIDERS="aws gcp" make kill clean cli deploy <<< $'7\n2'
```

### Example: same as above
```console
$ TAGS="aws gcp" make kill clean cli deploy <<< $'7\n2'
```

> Use `TAGS` environment to specify any/all supported build tags that also include conditionally linked remote backends (see next).

> Use `AIS_BACKEND_PROVIDERS` environment to select remote backends that include 3 (three) Cloud providers and `ht://` - namely: (`aws`, `gcp`, `azure`, `ht`)

> For the complete list of supported build tags, please see [conditional linkage](/docs/build_tags.md).

### Example: same as above but also build `aisnode` with debug info
```console
$ TAGS="aws gcp debug" make kill clean cli deploy <<< $'7\n2'
```

### Further:

* `make kill`    - terminate local AIStore.
* `make restart` - shut it down and immediately restart using the existing configuration.
* `make help`    - show make options and usage examples.

For even more development options and tools, please refer to:

* [development docs](/docs/development.md)

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

Other variables, such as [`AIS_PRIMARY_EP`](/docs/environment-vars.md#primary) and [`AIS_USE_HTTPS`](/docs/environment-vars.md#https) can prove to be useful at deployment time.

For developers, CLI `ais config cluster log.modules ec xs` (for instance) would allow to selectively raise and/or reduce logging verbosity on a per module bases - modules EC (erasure coding) and xactions (batch jobs) in this particular case.

> To list all log modules, type `ais config cluster log` (or `ais config node NODE inherited log`) and press `<TAB-TAB>`.

Finally, there's also HTTPS configuration (including **X.509** certificates and options), and the corresponding [environment](#tls-testing-with-self-signed-certificates).

For details, please refer to:

* [HTTPS: loading, reloading, and generating certificates; switching cluster between HTTP and HTTPS](/docs/https.md)

## Multiple deployment options

AIStore deploys anywhere anytime supporting multiple deployment options [summarized and further referenced here](/deploy/README.md).

All [containerized deployments](/deploy/README.md) have their own separate `Makefiles`. With the exception of [local playground](#local-playground), each specific build-able development (`dev/`) and production (`prod/`) option under the `deploy` folder contains a pair: {`Dockerfile`, `Makefile`}.

> This separation is typically small in size and easily readable and maintainable.

Also supported is the option *not* to have the [required](#prerequisites) [Go](https://go.dev) installed and configured. To still be able to build AIS binaries without [Go](https://go.dev) on your machine, make sure that you have `docker` and simply uncomment `CROSS_COMPILE` line in the top [`Makefile`](https://github.com/NVIDIA/aistore/blob/main/Makefile).

In the software, _type of the deployment_ is also present in some minimal way. In particular, to overcome certain limitations of [Local Playground](#local-playground) (single disk shared by multiple targets, etc.) - we need to know the _type_. Which can be:

| enumerated type | comment |
| --- | --- |
| `dev` | development |
| `k8s` | Kubernetes |
| `linux` | Linux |

> The most recently updated enumeration can be found in the [source](https://github.com/NVIDIA/aistore/blob/main/api/apc/const.go)

> The _type_ shows up in the `show cluster` output - see example above.

### Kubernetes deployments

For production deployments, we developed the [AIS/K8s Operator](https://github.com/NVIDIA/ais-k8s/tree/main/operator). This dedicated GitHub [repository](https://github.com/NVIDIA/ais-k8s) contains:

* [AIStore on Kubernetes](https://github.com/NVIDIA/ais-k8s)
* [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/blob/main/operator/README.md)
* [Ansible Playbooks](https://github.com/NVIDIA/ais-k8s/blob/main/playbooks/README.md)
* [Helm Charts](https://github.com/NVIDIA/ais-k8s/tree/main/helm)
- [Monitoring](https://github.com/NVIDIA/ais-k8s/blob/main/monitoring/README.md)

### Minimal all-in-one-docker Deployment

This option has the unmatched convenience of requiring an absolute minimum time and resources - please see this [README](/deploy/prod/docker/single/README.md) for details.

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

## Running AIStore in Google Colab

To quickly set up AIStore (with AWS and GCP backends) in a [Google Colab](https://colab.research.google.com/) notebook, use our ready-to-use [notebook](https://colab.research.google.com/github/NVIDIA/aistore/blob/main/python/examples/google_colab/aistore_deployment.ipynb): 

**Important Notes:**
- This sample installs Go v1.23.1, the supported Go version and toolchain at the time of writing.
- AIStore runs in the background. However, if you stop any cell, it sends a "SIGINT" (termination signal) to all background processes, terminating AIStore. To restart AIStore, simply rerun the relevant cell.

## Kubernetes Playground

In our development and testing, we make use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and the capability, further documented [here](/deploy/dev/k8s/README.md), to run the Kubernetes cluster on a single development machine. There's a distinct advantage that AIStore extensions that require Kubernetes - such as [Extract-Transform-Load](/docs/etl.md), for example - can be developed rather efficiently.

* [AIStore on Minikube](/deploy/dev/k8s/README.md)

## Setting Up HTTPS Locally

So far, all examples in this getting-started document run a bunch of local web servers that listen for plain HTTP and collaborate to provide clustered storage.

There's a separate document that tackles HTTPS topics that, in part, include:

- [Generating self-signed certificates](https.md#generating-self-signed-certificates)
- [Deploying: 4 targets, 1 gateway, 6 mountpaths, AWS backend](https.md#deploying-4-targets-1-gateway-6-mountpaths-aws-backend)
- [Accessing the cluster](https.md#accessing-the-cluster)
- [Testing with self-signed certificates](https.md#testing-with-self-signed-certificates)
- [Updating and reloading X.509 certificates](https.md#updating-and-reloading-x509-certificates)
- [Switching cluster between HTTP and HTTPS](https.md#switching-cluster-between-http-and-https)

## Build, Make, and Development Tools

As noted, the project utilizes GNU `make` to build and run things both locally and remotely (e.g., when deploying AIStore via [Kubernetes](/deploy/dev/k8s/Dockerfile). As the very first step, run `make help` for help on:

* **building** AIS node binary (called `aisnode`) deployable as both storage target **or** an ais gateway (most of the time referred to as "proxy");
* **building** [CLI](/docs/cli.md)
* **building** [benchmark tools](/bench/tools/README.md).

In particular, the `make` provides a growing number of developer-friendly commands to:

* **deploy** the AIS cluster on your local development machine;
* **run** all or selected tests;
* **instrument** AIS binary with race detection, CPU and/or memory profiling and more.

Of course, local build is intended for development only. For production, there is a separate [dedicated repository](https://github.com/NVIDIA/ais-k8s) noted below.

In summary:

* for development using _local playground_, please also see [local playground](https://github.com/NVIDIA/aistore/tree/main/deploy/dev/local);
* for docker and minikube builds supported by _this_ repository, see [docker and minikube](https://github.com/NVIDIA/aistore/tree/main/deploy) deployments;
* finally, for production build and deployment, please refer to the [ais-k8s repository](https://github.com/NVIDIA/ais-k8s).

### A note on conditional linkage

AIStore build supports conditional linkage of the supported remote backends: [S3, GCS, Azure, OCI](https://github.com/NVIDIA/aistore/blob/main/docs/images/cluster-block-v3.26.png).

> For the complete list of supported build tags, please see [conditional linkage](/docs/build_tags.md).

> For the most recently updated list, please see [3rd party Backend providers](/docs/providers.md).

To access remote data (and store it in-cluster), AIStore utilizes the respective provider's SDK.

> For Amazon S3, that would be `aws-sdk-go-v2`, for Azure - `azure-storage-blob-go`, and so on. Each SDK can be **conditionally linked** into `aisnode` executable - the decision to link or not to link is made prior to deployment.

But not only supported remote backends are conditionally linked. Overall, the following list of commented examples presents almost all supported build tags (with maybe one minor exception):

```console
# 1) build aisnode with no build tags, no debug
$ MODE="" make node

# 2) build aisnode with no build tags but with debug
$ MODE="debug" make node

# 3) all 4 cloud backends, no debug
$ AIS_BACKEND_PROVIDERS="aws azure gcp oci" MODE="" make node

# 4) cloud backends, with debug
$ AIS_BACKEND_PROVIDERS="aws azure gcp oci" MODE="debug" make node

# 5) cloud backends, debug, statsd
## Note: if `statsd` build tag is not specified `aisnode` will get built with Prometheus support.
## For more information (including the binary choice between StatsD and Prometheus), please see docs/metrics.md and docs/prometheus.md
$ TAGS="aws azure gcp statsd debug" make node

# 6) statsd, debug, nethttp (note that fasthttp is used by default)
$ TAGS="nethttp statsd debug" make node
```

In addition, to build [AuthN](/docs/authn.md), [CLI](/docs/cli.md), and/or [aisloader](/docs/aisloader.md), run:

* `make authn`
* `make cli`
* `make aisloader`

respectively. With each of these `make`s, you can also use `MODE=debug` - debug mode is universally supported.


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

## Assorted Curl

Some will say that using AIS [CLI](/docs/cli.md) with aistore is an order of magnitude more convenient than [curl](https://curl.se/). Or two orders.

Must be a matter of taste, though, and so here are a few `curl` examples.

> As always, `http://localhost:8080` address (below) simply indicates [Local Playground](#local-playground) and must be understood as a placeholder for an _arbitrary_ aistore endpoint (`AIS_ENDPOINT`).

### Example: PUT via aistore [S3 interface](/docs/s3compat.md); specify PUT content inline (in the curl command):

```console
$ ais create ais://nnn ## create bucket, if doesn't exist

$ curl -L -X PUT -d "0123456789" http://localhost:8080/s3/nnn/qqq

$ ais ls ais://nnn
NAME     SIZE
qqq      10B
```

### Example: same as above using [Easy URL](/docs/http_api.md#easy-url)

```console
## notice PROVIDER/BUCKET/OBJECT notation
##
$ curl -L -X PUT -d "0123456789" http://localhost:8080/ais/nnn/eee

$ ais ls ais://nnn
NAME     SIZE
eee      10B
qqq      10B
```

### Finally, same as above using native aistore API

```console
## notice '/v1/objects' API endpoint
##
$ curl -L -X PUT -d "0123456789" http://localhost:8080/v1/objects/nnn/uuu

$ ais ls ais://nnn
NAME     SIZE
eee      10B
qqq      10B
uuu      10B
```
