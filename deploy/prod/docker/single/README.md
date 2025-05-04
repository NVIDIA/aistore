# Minimal Standalone Docker Deployment

The idea is to quickly run a fully functional (albeit minimal) AIS cluster consisting of a single gateway and a single storage node.

This text explains How. Running it should take less than a minute, after which you can execute a few assorted commands, e.g.:

```console
$ AIS_ENDPOINT=http://localhost:51080 ais show cluster

$ AIS_ENDPOINT=http://localhost:51080 ais show config cluster

$ AIS_ENDPOINT=http://localhost:51080 ais show log <NODE>

$ export AIS_ENDPOINT="http://localhost:51080"
$ ais create ais://abc
```

and [so on](/docs/cli.md).

## Table of Contents
- [Prerequisites](#prerequisites)
  - [Docker](#docker)
  - [CLI](#cli)
- [How to Build](#how-to-build)
- [How to Deploy](#how-to-deploy)
  - [Multiple Disks](#multiple-disks)
  - [Backend Provider Setup](#backend-provider-setup)
  - [Cloud Deployment](#cloud-deployment)
- [Rebuilding](#rebuilding)
- [Shutting down](#shutting-down)

## Prerequisites

The prerequisites boil down to having **a)** docker and **b)** [AIS CLI](/docs/cli.md)

### <ins>Docker

If not already installed, install [Docker](https://docs.docker.com/engine/install/) on the machine that will be used to run your containerized AIS cluster. Verify that Docker has been correctly installed with the following script:

```console
$ docker run hello-world

Hello from Docker!
This message shows that your installation appears to be working correctly.
...
```

The Docker image used to deploy AIS clusters in this guide is `aistorage/cluster-minimal:latest`, which can be found [here](https://hub.docker.com/repository/docker/aistorage/cluster-minimal) on Docker Hub. `aistorage/cluster-minimal:latest` is an all-in-one, custom Docker image with AIS resources pre-installed.

### <ins>CLI

The easiest might be to clone https://github.com/NVIDIA/aistore, `cd aistore`, and `make cli`.

Or, you can also download the latest released CLI binary from the [release assets](https://github.com/NVIDIA/aistore/releases).

Further references:

* [AIS CLI](/docs/cli.md)
* [CLI Documentation](https://github.com/NVIDIA/aistore/tree/main/docs/cli)

## How to Build

First of, you can always pull docker hub for the existing (official) image:

```
docker pull aistorage/cluster-minimal:latest
```

But there may be any number of reasons to build it from (the latest) sources and/or with alternative environment settings.

In which case:

```
# build `cluster-minimal:test` and add it to local registry called `aistorage`
#
$ IMAGE_REPO=aistorage/cluster-minimal IMAGE_TAG=test make -e build
```

Alternatively, you could build _and_ upload custom image to a selected registry of choice (DockerHub, AWS, GitLab Container Registry, etc.), e.g.:

```console
# build `cluster-minimal:custom` and push it to docker hub repository called `my-docker-rep`
#
$ REGISTRY_URL=docker.io IMAGE_REPO=my-docker-rep IMAGE_TAG=custom make -e all
```

## How to Deploy

### <ins>Minimal Setup

The following command starts an AIS cluster in a Docker container with a single disk (requires at least one disk) that is mounted under a temporary directory on the host:

```console
$ docker run -d -p 51080:51080 -v $(mktemp -d):/ais/disk0 aistorage/cluster-minimal:latest
```

> Note the command exposes the host `51080` port. It is possible to reach the cluster with `http://localhost:51080` if you are on the host machine.

> The above command, and all subsequent commands, assume that all volumes will be mounted in `/ais/*` directory.

You can check the status of the AIS cluster on the now running Docker instance (using local host endpoint), as follows:

```
$ AIS_ENDPOINT="http://localhost:51080" ais show cluster
```

> The `ais` command (above) is AIS command line management tool often simply referred to as [CLI](/docs/cli.md). To build it from sources, run `make cli`. To install one of the released binaries, see `scripts/install_from_binaries.sh --help`.


In the `ais show cluster` output - a sample below - notice software versions and various other minor details. But most of all note that this is one truly minimalistic cluster: a single gateway and a single storage node:

```console
$ AIS_ENDPOINT="http://localhost:51080" ais show cluster
$ ais show cluster
PROXY            MEM USED(%)     MEM AVAIL       LOAD AVERAGE    UPTIME  STATUS
p[nnDIlKOE][P]   0.23%           18.21GiB        [1.5 2.8 1.9]   -       online

TARGET           MEM USED(%)     MEM AVAIL   CAP USED(%)   CAP AVAIL    LOAD AVERAGE    REBALANCE   UPTIME  STATUS  ALERT
t[BKCheLNU]      0.22%           18.21GiB    27%           68.445GiB    [1.5 2.8 1.9]   -           -       online  cluster-not-started-yet

Summary:
   Proxies:             1
   Targets:             1 (one disk)
   Capacity:            used 25.49GiB (27%), available 68.44GiB
   Cluster Map:         version 2, UUID iHjYXi5IL, primary p[nnDIlKOE]
   Software:            3.24.rc3.1f74b8f4b (build: 2024-08-23T14:09:13+0000)
   Deployment:          linux
   Status:              2 online
   Rebalance:           n/a
   Authentication:      disabled
   Version:             3.24.rc3.1f74b8f4b
   Build:               2024-08-23T14:09:13+0000
```

> **IMPORTANT**: `docker stop` may not be the right way to stop `cluster-minimal` instance. Run `ais cluster shutdown --yes` to shut down the cluster gracefully. For details, see section [Shutting down](#shutting-down) below.

### <ins>Multiple Disks

In this section, we show how to run all-in-one-docker AIS cluster with virtual disks that we also create right away.

But first:

> **NOTE**: mounted disk paths must resolve to _distinct_ and _disjoint_ file systems. AIStore checks and enforces non-sharing of local filesystems: each target [mountpath](/docs/overview.md#terminology) must have its own, undivided.

And of course, when there are no spare drives for data storage, one uses loopback, as follows:

```console
$ for i in {1..4}; do deploy/dev/loopback.sh --mountpath /tmp/ais/mp$i --size 1G; done
```

At this point, we have 4(file-based)  disks, and we can use them:

```console
$ docker run -d -p 51080:51080 \
  -v /tmp/ais/mp1:/ais/disk1 -v /tmp/ais/mp2:/ais/disk2 \
  -v /tmp/ais/mp3:/ais/disk3 -v /tmp/ais/mp4:/ais/disk4 \
  aistorage/cluster-minimal:latest

$ $ ais storage mountpath
yynRQpXV
        Used: min= 6%, avg= 6%, max= 6%
                                        /ais/disk1 /dev/loop10(ext4)
```

Notice (above): upon startup, the cluster shows a single disk. That's because initial (hardcoded) configuration that we provided specifies only one.

This is easily fiable, though: AIStore supports adding [mountpath](/docs/overview.md#terminology) at runtime. More exactly, all 4 verbs are supported: attach, detach, enable, disable.

```console
$ ais storage mountpath attach t[yynRQpXV] /ais/disk2
$ ais storage mountpath attach t[yynRQpXV] /ais/disk3
$ ais storage mountpath attach t[yynRQpXV] /ais/disk4

$ ais storage mountpath
yynRQpXV
        Used: min= 6%, avg= 6%, max= 6%
                                        /ais/disk1 /dev/loop10(ext4)
                                        /ais/disk2 /dev/loop18(ext4)
                                        /ais/disk3 /dev/loop19(ext4)
                                        /ais/disk4 /dev/loop20(ext4)
```

### <ins>Backend Provider Setup

By default, `cluster-minimal` gets built with two selected providers of remote backends: GCP and AWS.

But you can change this default - see `Dockerfile` in this directory for `AIS_BACKEND_PROVIDERS`.

Secondly, and separately, you could also deploy `cluster-minimal` with a subset of linked-in providers.

In other words, there's the flexibility to *activate* only some of the built-in providers at deployment time, e.g.:

``` console
$ docker run -d -p 51080:51080 -v $(mktemp -d):/ais/disk0 -e AIS_BACKEND_PROVIDERS="gcp" aistorage/cluster-minimal:latest
```

> **IMPORTANT**: For both AWS or GCP usage, to ensure the cluster works properly with backend providers, it is _essential_ to pass the environment variable `AIS_BACKEND_PROVIDERS`, a space-separated list of support backend provides to be used, in your `docker run` command.

**AWS Backend**

The easiest way to pass your credentials to the AIS cluster is to mount a volume prepared with a [`config file`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) and provide `AWS_CONFIG_FILE`, the path to `config` in your `docker run` command:

```
docker run -d \
  -p 51080:51080 \
  -v <path_to_aws_config>:/path/to/config \
  -e AIS_CONFIG_FILE="/path/to/config" \
  -e AIS_BACKEND_PROVIDERS="aws" \
  -v /disk0:/ais/disk0 \
  aistorage/cluster-minimal:latest
```

Alternatively, it is possible to explicitly pass the credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`) explicitly as [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) in your `docker run` command:

```
docker run -d \
  -p 51080:51080 \
  -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
  -e AWS_SECRET_ACCESS_KEY="wJalrXUtfdfUYBjdtEnFxEMEXAMPLE" \
  -e AWS_REGION="us-west-2" \
  -e AIS_BACKEND_PROVIDERS="aws" \
  -v /disk0:/ais/disk0 \
  aistorage/cluster-minimal:latest
```

Once the container is running and the cluster is deployed, you can verify that your AWS buckets are accessible by the newly deployed AIS cluster:

```console
$ AIS_ENDPOINT="http://localhost:51080" ais ls

AWS Buckets (28)
  aws://sample-bucket-1
  aws://sample-bucket-2
  aws://sample-bucket-3
...
```

**GCP Backend**

The following command deploys a containerized AIS cluster with GCP by providing a volume with the `config file` and a path to the `config`:

```
docker run -d \
  -p 51080:51080 \
  -v <path_to_gcp_config>.json:/credentials/gcp.json \
  -e GOOGLE_APPLICATION_CREDENTIALS="/credentials/gcp.json" \
  -e AIS_BACKEND_PROVIDERS="gcp" \
  -v /disk0:/ais/disk0 \
  aistorage/cluster-minimal:latest
```

### <ins>Cloud Deployment

Minimal deployments of AIS clusters can also be done on cloud compute instances, such as those provided by AWS EC2. Containerized deployment on a cloud compute cluster may be an appealing option for those wishing to continually run an AIS cluster in the background.

 [`ssh`](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) into your EC2 instance and deploy an AIS cluster (as shown above). The following command creates a containerized AIS cluster with one mounted volume hosted locally:

```
docker run -d -p 51080:51080 -v /ais/sdf:/ais/disk0 aistorage/cluster-minimal:latest
```

**Accessing Cluster from EC2 Host Instance**

From the EC2 instance's `bash`, locally access your cluster via the following command:

```
AIS_ENDPOINT="http://<ip-address>:51080" ais show cluster
```

> Run `ifconfig` on your EC2 instance to find an available IP address to be used for local host access to the cluster.

**Accessing Cluster Remotely**

Additionally, any workstation with an IP in the EC2 instance's list of allowed IP addresses *can* remotely access the EC2-hosted AIS cluster using the instance's AWS host name as follows:

```
AIS_ENDPOINT="http://<ec2-host-name>:51080" ais show cluster
```

**EC2 Minimal Deployment Benchmarks**

For more information on deployment performance, please refer [here](./ec2-standalone-benchmark.md).

## Rebuilding

The provided [Makefile](Makefile) and [Dockerfile](Dockerfile) are the bare minimum "stripped-down" versions that you may find insufficient one way or another.

One common reason for this could be: credentials to facilitate access to a given remote backend - say, Amazon S3.

Here's an example:

```diff
diff --git a/deploy/prod/docker/single/Dockerfile b/deploy/prod/docker/single/Dockerfile
index 9f7d048cf..d775fc69d 100644
--- a/deploy/prod/docker/single/Dockerfile
+++ b/deploy/prod/docker/single/Dockerfile
@@ -42,6 +42,9 @@ COPY aisnode_config.sh ./
 COPY limits.conf /etc/security/limits.conf
 COPY --from=builder /go/bin/aisnode bin/

+RUN mkdir -p /root/.aws
+COPY credentials /root/.aws/.
+
 EXPOSE 51080/tcp

 ENTRYPOINT ["sh", "-c", "entrypoint/entrypoint.sh \"$@\"", "--"]
--- a/deploy/prod/docker/single/Makefile
+++ b/deploy/prod/docker/single/Makefile
@@ -14,10 +14,11 @@ all: build push
 build:
        cp ${AISTORE_PATH}/deploy/dev/utils.sh .
        cp ${AISTORE_PATH}/deploy/dev/local/aisnode_config.sh .
        cp ${AISTORE_PATH}/deploy/conf/limits.conf .
+       cp ~/.aws/credentials .

-       docker build -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile . || rm -f utils.sh aisnode_config.sh limits.conf
+       docker build -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile . || rm -f utils.sh aisnode_config.sh limits.conf credentials

-       rm -f utils.sh aisnode_config.sh limits.conf
+       rm -f utils.sh aisnode_config.sh limits.conf credentials

 push:
        docker push $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG)
```

This, essentially, single-line change followed by `make -e build` will have the result of injecting your own AWS S3 credentials into the new `cluster-minimal` image. Once done and [deployed](#how-to-deploy), use CLI to list your S3 buckets:

```console
$ AIS_ENDPOINT="http://localhost:51080" ais ls s3
```

and, generally, start using `cluster-minimal` to transparently work with those (buckets).

**IMPORTANT:**

> For obvious reasons, makes sense to carefully consider implications before sharing (pushing, uploading) the image that contains any sort of secrets.

> In the `make -e` command, optionally specify `IMAGE_TAG` to differentiate your custom-built image from the default.

## Shutting down

Storage clusters are usually quite persistent: they may store all sorts of state information on disk and, upon restart, check the latter for consistency. Which is why, notwithstanding that `cluster-minimal` runs in a single docker container, it is important to shut it down properly:

```console
$ AIS_ENDPOINT=http://localhost:51080 ais cluster shutdown
```

Or, if you don't envision using this (or any other AIS cluster) any longer, _decommission_ it as follows (but first, check `--help` for details):

```console
$ AIS_ENDPOINT=http://localhost:51080 ais cluster decommission
```

In a sense, `cluster-minimal` is no different from a large bare-metal cluster deployed for production - the same rules apply.
