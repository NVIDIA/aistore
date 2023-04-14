# Minimal Production-Ready Standalone Docker Deployment

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
  - [docker](#docker)
  - [CLI](#cli)
- [How to Build](#how-to-build)
- [How to Deploy](#how-to-deploy)
  - [Multiple Disk Setup](#multiple-disk-setup)
  - [Backend Provider Setup](#backend-provider-setup)
  - [Cloud Deployment](#cloud-deployment)
- [How to Rebuild](#how-to-rebuild)

## Prerequisites

The prerequisites boil down to having **a)** docker and **b)** [AIS CLI](/docs/cli.md)

### <ins>docker

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
* [CLI Documentation](https://github.com/NVIDIA/aistore/tree/master/docs/cli)

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

```
docker run -d \
  -p 51080:51080 \
  -v $(mktemp -d):/ais/disk0 \
  aistorage/cluster-minimal:latest
```

> Note the command exposes the host `51080` port. It is possible to reach the cluster with `http://localhost:51080` if you are on the host machine.

> The above command, and all subsequent commands, assume that all volumes will be mounted in `/ais/*` directory.

You can check the status of the AIS cluster on the now running Docker instance (using local host endpoint) as follows:

```
$ AIS_ENDPOINT="http://localhost:51080" ais show cluster

PROXY                   MEM USED %  MEM AVAIL   UPTIME
proxy-0934deff64b7[P]   0.40%       7.78GiB     3m30s

TARGET              MEM USED %  MEM AVAIL   CAP USED %  CAP AVAIL   CPU USED %  REBALANCE   UPTIME
target-0934deff64b7 0.41%       7.78GiB     84%         8.950TiB    0.07%       -           3m30s

Summary:
 Proxies:       1 (0 - unelectable)
 Targets:       1
 Primary Proxy: proxy-0934deff64b7
 Smap Version:  3
```

### <ins>Multiple Disk Setup

You can also mount multiple disks to your containerized AIS cluster. The following command launches a local Docker instance of an AIS cluster, but with three disks mounted:

```
docker run -d \
  -p 51080:51080 \
  -v /disk0:/ais/disk0 \
  -v /disk1:/ais/disk1 \
  -v /some/disk2:/ais/disk2 \
  aistorage/cluster-minimal:latest
```

> **IMPORTANT**: The mounted disk paths must resolve to _distinct_ and _disjoint_ file systems. Otherwise, the setup may be corrupted.


### <ins>Backend Provider Setup

By default, `cluster-minimal` gets built with two selected providers of remote backends: GCP and AWS.

But you can change this default - see `Dockerfile` in this directory for `AIS_BACKEND_PROVIDERS`.

Secondly, and separately, you could also deploy `cluster-minimal` with a subset of linked-in providers.

In other words, there's the flexibility to *activate* only some of the built-in providers at deployment time, e.g.:

```
docker run -d -p 51080:51080 -v $(mktemp -d):/ais/disk0 -e AIS_BACKEND_PROVIDERS="gcp" aistorage/cluster-minimal:latest
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

Alternatively, it is possible to explicitly pass the credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) explicitly as [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) in your `docker run` command:

```
docker run -d \
  -p 51080:51080 \
  -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
  -e AWS_SECRET_ACCESS_KEY="wJalrXUtfdfUYBjdtEnFxEMEXAMPLE" \
  -e AWS_DEFAULT_REGION="us-west-2" \
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

## How to Rebuild

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
        cp ${AISTORE_PATH}/deploy/dev/local/aisnode_config.sh .
        cp ${AISTORE_PATH}/deploy/conf/limits.conf .
+       cp ~/.aws/credentials .

-       docker build -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile . || rm -f aisnode_config.sh limits.conf
+       docker build -t $(REGISTRY_URL)/$(IMAGE_REPO):$(IMAGE_TAG) -f Dockerfile . || rm -f aisnode_config.sh limits.conf credentials

-       rm -f aisnode_config.sh limits.conf
+       rm -f aisnode_config.sh limits.conf credentials

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
