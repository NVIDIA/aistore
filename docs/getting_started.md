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

* [Linux](#Linux) distribution with `GCC`, `sysstat`, `attr` and `util-linux` packages(**)
* Linux kernel 5.15+
* [Go 1.21 or later](https://golang.org/dl/)
* Extended attributes (`xattrs` - see next section)
* Optionally, Amazon (AWS), Google Cloud Platform (GCP), and/or Azure Cloud Storage accounts.

> (**) [Mac](#macOS) is also supported albeit in a limited (development only) way.

See also:

* Section [assorted command lines](#assorted-command-lines), and
* `CROSS_COMPILE` comment below.

### Linux

Depending on your Linux distribution, you may or may not have `GCC`, `sysstat`, and/or `attr` packages. These packages must be installed.

Speaking of distributions, our current default recommendation is Ubuntu Server 20.04 LTS. But Ubuntu 18.04 and CentOS 8.x (or later) will also work. As well as numerous others.

For the [local filesystem](/docs/performance.md), we currently recommend xfs. But again, this (default) recommendation shall not be interpreted as a limitation of any kind: other fine choices include zfs, ext4, f2fs and more.

Since AIS itself provides n-way mirroring and erasure coding, hardware RAID would _not_ be recommended. But can be used, and will work.

The capability called [extended attributes](https://en.wikipedia.org/wiki/Extended_file_attributes), or `xattrs`, is a long-time POSIX legacy supported by all mainstream filesystems with no exceptions. Unfortunately, `xattrs` may not always be enabled in the Linux kernel configurations - the fact that can be easily found out by running the `setfattr` command.

If disabled, please make sure to enable xattrs in your Linux kernel configuration. To quickly check:

```console
$ touch foo
$ setfattr -n user.bar -v ttt foo
$ getfattr -n user.bar foo
```

### macOS

For developers, there's also macOS aka Darwin option. Certain capabilities related to querrying the state and status of local hardware resources (memory, CPU, disks) may be missing. In fact, it is easy to review specifics with a quick check on the sources:

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

## Table of Contents

- [Local Playground](#local-playground)
  - [From source](#from-source)
  - [Running Local Playground with emulated disks](#running-local-playground-with-emulated-disks)
  - [Running Local Playground remotely](#running-local-playground-remotely)
- [Make](#make)
- [System environment variables](#system-environment-variables)
- [Multiple deployment options](#multiple-deployment-options)
  - [Kubernetes deployments](#kubernetes-deployments)
  - [Minimal all-in-one-docker Deployment](#minimal-all-in-one-docker-deployment)
  - [Testing your cluster](#testing-your-cluster)
- [Kubernetes Playground](#kubernetes-playground)
- [Setting Up HTTPS Locally](#setting-up-https-locally)
- [Build, Make, and Development Tools](#build-make-and-development-tools)
  - [A note on conditional linkage](#a-note-on-conditional-linkage)
- [Containerized Deployments: Host Resource Sharing](#containerized-deployments-host-resource-sharing)
- [Assorted command lines](#assorted-command-lines)

## Local Playground

If you're looking for speedy evaluation, want to experiment with [supported features](https://github.com/NVIDIA/aistore/tree/main?tab=readme-ov-file#features), get a feel of initial usage, or development - for any and all of these reasons running AIS from its GitHub source might be a good option.

Hence, we introduced and keep maintaining **Local Playground** - one of the several supported [deployment options](#multiple-deployment-options).

> Some of the most popular deployment options are also **summarized** in this [table](https://github.com/NVIDIA/aistore/tree/main/deploy#readme). The list includes Local Playground, and its complementary guide [here](https://github.com/NVIDIA/aistore/blob/main/deploy/dev/local/README.md).

> Local Playground is **not intended** for production and is not meant to provide optimal performance.

To run AIStore from source, one would typically need to have **Go**: compiler, linker, tools, and required packages. However:

> `CROSS_COMPILE` option (see below) can be used to build AIStore without having (to install) [Go](https://golang.org/dl/) and its toolchain (requires Docker).

To install Go(lang) on Linux:

* Download the latest `go1.21.<x>.linux-amd64.tar.gz` from [Go downloads](https://golang.org/dl/)
* Follow [installation instructions](https://go.dev/doc/install)
* **Or** simply run: `tar -C /usr/local -xzf go1.21.<x>.linux-amd64.tar.gz` and add `/usr/local/go/bin` to $PATH

Next, if not done yet, export the [`GOPATH`](https://go.dev/doc/gopath_code#GOPATH) environment variable.

Here's an additional [5-minute introduction](/deploy/dev/local/README.md) that talks more in-depth about setting up the Go environment variables.

Once done, we can run AIS as follows:


## Step 1: Clone the AIStore repository and preload dependencies

We want to clone the repository into the following path so we can access
some of the associated binaries through the environment variables we set up earlier.

```console
$ cd $GOPATH/src/github.com/NVIDIA
$ git clone https://github.com/NVIDIA/aistore.git
$ cd aistore
# Optionally, run `make mod-tidy` to preload dependencies
```

## Step 2: Deploy cluster and verify the running status using `ais` cli

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

### `clean_deploy.sh`

Alternatively (to `make deploy`) or, rather, in addition, one can also use:

* [`clean_deploy.sh`](https://github.com/NVIDIA/aistore/blob/main/scripts/clean_deploy.sh)
* [`clean_deploy.sh readme`](/docs/development.md#clean-deploy)

With no arguments, this script also builds AIStore binaries (such as `aisnode` and `ais` CLI). You can pass in arguments to configure the same options that the `make deploy` command above uses.

```console
$ ./scripts/clean_deploy.sh --target-cnt 1 --proxy-cnt 1 --mountpath-cnt 1 --deployment local --cleanup
```

## Step 3: Run `aisloader` tool

We can now run the `aisloader` tool to benchmark our new cluster.

```console
$ make aisloader # build aisloader tool

$ aisloader -bucket=ais://abc -duration 2m -numworkers=8 -minsize=1K -maxsize=1K -pctput=100 --cleanup=false # run aisloader for 2 minutes (8 workers, 1KB size, 100% write, no cleanup)
```

## Step 4: Run iostat (or use any of the multiple [documented](/docs/prometheus.md) ways to monitor AIS performance)

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

### Example: shutdown/cleanup, and then deploy non-interactively a cluster consisting of 7 targets (4 mountpaths each) and 2 proxies; build `aisnode` executable with GCP and AWS backends
```console
$ make kill clean deploy <<< $'7\n2\n4\ny\ny\nn\n0\n'
```

### Example: same as above
```console
$ AIS_BACKEND_PROVIDERS="aws gcp" make kill clean deploy <<< $'7\n2'
```

### Example: same as above
```console
$ TAGS="aws gcp" make kill clean deploy <<< $'7\n2'
```

> Use `TAGS` environment to specify any/all supported build tags that also include conditionally linked remote backends (see next).

> Use `AIS_BACKEND_PROVIDERS` environment to select remote backends that include 3 (three) Cloud providers and `ht://` - namely: (`aws`, `gcp`, `azure`, `ht`)

### Example: same as above but also build `aisnode` with debug info
```console
$ TAGS="aws gcp debug" make kill clean deploy <<< $'7\n2'
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

Other variables, such as [`AIS_PRIMARY_EP`](environment-vars.md#primary) and [`AIS_USE_HTTPS`](environment-vars.md#https) can prove to be useful at deployment time.

For developers, CLI `ais config cluster log.modules ec xs` (for instance) would allow to selectively raise and/or reduce logging verbosity on a per module bases - modules EC (erasure coding) and xactions (batch jobs) in this particular case.

> To list all log modules, type `ais config cluster` or `ais config node` and press `<TAB-TAB>`.

Finally, there's also HTTPS configuration (including **X.509** certificates and options), and the corresponding [environment](#tls-testing-with-self-signed-certificates).

For details, please see section [TLS: testing with self-signed certificates](#tls-testing-with-self-signed-certificates) below.

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

> The most recently updated enumeration can be found in the [source](https://github.com/NVIDIA/aistore/blob/main/ais/utils.go#L329)

> The _type_ shows up in the `show cluster` output - see example above.

### Kubernetes deployments

For any Kubernetes deployments (including, of course, production deployments) please use a separate and dedicated [AIS-K8s GitHub](https://github.com/NVIDIA/ais-k8s/blob/main/docs/README.md) repository.
The repo contains detailed [Ansible playbooks](https://github.com/NVIDIA/ais-k8s/tree/main/playbooks) that cover a variety of use cases and configurations.

Finally, the [repository](https://github.com/NVIDIA/ais-k8s) hosts the [Kubernetes Operator](https://github.com/NVIDIA/ais-k8s/tree/master/operator) project that will eventually replace Helm charts and will become the main deployment, lifecycle, and operation management "vehicle" for AIStore.

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

## Kubernetes Playground

In our development and testing, we make use of [Minikube](https://kubernetes.io/docs/tutorials/hello-minikube/) and the capability, further documented [here](/deploy/dev/k8s/README.md), to run the Kubernetes cluster on a single development machine. There's a distinct advantage that AIStore extensions that require Kubernetes - such as [Extract-Transform-Load](/docs/etl.md), for example - can be developed rather efficiently.

* [AIStore on Minikube](/deploy/dev/k8s/README.md)

## Setting Up HTTPS Locally

In the end, all examples above run a bunch of local web servers that listen for plain HTTP requests. Following are quick steps for developers to engage HTTPS.

This is still a so-called _local playground_ type deployment _from scratch_, whereby we are not trying to switch an existing cluster from HTTP to HTTPS, or vice versa. All we do here is deploying a brand new HTTPS-based aistore.

> Note: If you need to switch an existing AIS cluster to HTTPS, please refer to [these steps](switch_https.md).

### Generate Certificates

Creating a self-signed certificate along with its private key and a Certificate Authority (CA) certificate using [OpenSSL](https://www.openssl.org/) involves steps:

* First, we create a self-signed Certificate Authority (CA) certificate (`ca.crt`).
* Second, create a Certificate Signing Request (CSR).
* Finally, based on CSR and CA we create the server certs (`server.key` and `server.crt`) - as follows:

```console
openssl req -x509 -newkey rsa:2048 -keyout ca.key -out ca.crt -days 1024 -nodes -subj "/CN=localhost" -extensions v3_ca -config <(printf "[req]\ndistinguished_name=req\nx509_extensions=v3_ca\n[ v3_ca ]\nsubjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1\nbasicConstraints=CA:TRUE\n")

openssl req -new -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/C=US/ST=California/L=Santa Clara/O=NVIDIA/OU=AIStore/CN=localhost" -config <(printf "[req]\ndistinguished_name=req\nreq_extensions = v3_req\n[ v3_req ]\nsubjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1\n")

openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256 -extfile <(printf "[ext]\nsubjectAltName=DNS:localhost,DNS:127.0.0.1,IP:127.0.0.1\nbasicConstraints=CA:FALSE\nkeyUsage=digitalSignature,nonRepudiation,keyEncipherment,dataEncipherment\nextendedKeyUsage=serverAuth,clientAuth\n") -extensions ext
```

> **Important:** make sure to specify correct DNS. For local deployments, `localhost` and `127.0.0.1` domain must be provided - otherwise certificate validation will fail.

### Deploy Cluster: 4 targets, 1 gateway, 6 mountpaths, AWS backend

```console
## shutdown previous running AIS cluster
##
$ make kill

## cleanup
##
$ make clean

## run cluster: 4 targets, 1 gateway, 6 mountpaths, AWS backend, HTTPS
##
$ TAGS=aws AIS_USE_HTTPS=true AIS_SKIP_VERIFY_CRT=true AIS_SERVER_CRT=<path-to-cert>/server.crt AIS_SERVER_KEY=<path-to-key>/server.key make deploy <<< $'4\n1\n6'
```
> Notice environment variables above: **AIS_USE_HTTPS**, **AIS_SKIP_VERIFY_CRT**, **AIS_SERVER_CRT** and **AIS_SERVER_KEY**.

> Also note that `<path-to-cert>` (above) must not necessarily be absolute. Assuming, you have `server.*` in your **local directory**, the following will work as well:

```console
$ TAGS=aws AIS_USE_HTTPS=true AIS_SKIP_VERIFY_CRT=true AIS_SERVER_CRT=server.crt AIS_SERVER_KEY=server.key make deploy <<< $'4\n1\n6'
```

### Accessing the Cluster

To use CLI, try first any command with HTTPS-based cluster endpoint, for instance:

```console
$ AIS_ENDPOINT=https://127.0.0.1:8080 ais show cluster
```

But if it fails with "failed to verify certificate" message, perform a simple step to configufre CLI to skip HTTPS cert validation:

```console
$ ais config cli set cluster.skip_verify_crt true
"cluster.skip_verify_crt" set to: "true" (was: "false")
```

And then try again.

### TLS: testing with self-signed certificates

In the previous example, the cluster is simply ignoring SSL certificate verification due to `AIS_SKIP_VERIFY_CRT` being `true`. For a more secure setup, consider validating certificates by configuring the necessary environment variables as shown in the table below- 

| var name | description | the corresponding cluster configuration |
| -- | -- | -- |
| `AIS_USE_HTTPS`          | when false, we use plain HTTP with all the TLS config (below) simply **ignored** | "net.http.use_https" |
| `AIS_SERVER_CRT`         | aistore cluster X.509 certificate | "net.http.server_crt" |
| `AIS_SERVER_KEY`         | certificate's private key | "net.http.server_key"|
| `AIS_DOMAIN_TLS`         | NOTE: not supported, must be empty (domain, hostname, or SAN registered with the certificate) | "net.http.domain_tls"|
| `AIS_CLIENT_CA_TLS`      | Certificate authority that authorized (signed) the certificate | "net.http.client_ca_tls" |
| `AIS_CLIENT_AUTH_TLS`    | Client authentication during TLS handshake: a range from 0 (no authentication) to 4 (request and validate client's certificate) | "net.http.client_auth_tls" |
| `AIS_SKIP_VERIFY_CRT`    | when true: skip X.509 cert verification (usually enabled to circumvent limitations of self-signed certs) | "net.http.skip_verify" |

> More info on [`AIS_CLIENT_AUTH_TLS`](https://pkg.go.dev/crypto/tls#ClientAuthType).

In the following example, we run https based deployment where `AIS_SKIP_VERIFY_CRT` is `false`.

```console
$ make kill
$ # delete smaps
$ find ~/.ais* -type f -name ".ais.smap" | xargs rm
$ # substitute varibles in below files to point to correct certificates
$ source ais/test/tls-env/server.conf
$ source ais/test/tls-env/client.conf
$ AIS_USE_HTTPS=true make deploy <<< $'6\n6\n4\ny\ny\nn\n\n'
```

Notice that when the cluster is first time deployed `server.conf` environment (above) overrides aistore cluster configuration.

> Environment is ignored upon cluster restarts and upgrades.

On the other hand, `ais/test/tls-env/client.conf` contains environment variables to override CLI config. The correspondence between environment and config names is easy to see as well.

> See also: [Client-side TLS environment](/docs/cli.md#environment-variables)

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

AIStore build supports conditional linkage of the supported remote backends: [S3, GCS, Azure](https://github.com/NVIDIA/aistore/blob/main/docs/images/cluster-block-2024.png).

> For the most recently updated list, please see [3rd party Backend providers](/docs/providers.md).

To access remote data (and store it in-cluster), AIStore utilizes the respective provider's SDK.

> For Amazon S3, that would be `aws-sdk-go-v2`, for Azure - `azure-storage-blob-go`, and so on. Each SDK can be **conditionally linked** into `aisnode` executable - the decision to link or not to link is made prior to deployment.

But not only supported remote backends are conditionally linked. Overall, the following list of commented examples presents almost all supported build tags (with maybe one minor exception):

```console
# 1) build aisnode with no build tags, no debug
$ MODE="" make node

# 2) build aisnode with no build tags but with debug
$ MODE="debug" make node

# 3) all 3 cloud backends, no debug
$ AIS_BACKEND_PROVIDERS="aws azure gcp" MODE="" make node

# 4) cloud backends, with debug
$ AIS_BACKEND_PROVIDERS="aws azure gcp" MODE="debug" make node

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
