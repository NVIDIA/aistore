# Minimal Production-Ready Standalone Docker Deployment

## Prerequisites

If not already installed, install [Docker](https://docs.docker.com/engine/install/) on the machine that will be used to run your containerized AIS cluster. Verify that Docker has been correctly installed with the following script:

```
$ docker run hello-world

Hello from Docker!
This message shows that your installation appears to be working correctly.
...
```

The Docker image used to deploy AIS clusters in this guide is `aistore/cluster-minimal:latest`, which can be found [here](https://hub.docker.com/repository/docker/aistore/cluster-minimal) on Docker Hub. `aistore/cluster-minimal:latest` is an all-in-one, custom Docker image with AIS resources pre-installed.

## How to Deploy

### <ins>Minimal Setup

The following command starts an AIS cluster in a Docker container with a single disk (requires at least one disk) that is mounted under a temporary directory on the host:

```
$ docker run -d \
 -p 51080:51080 \
 -v $(mktemp -d):/ais/disk0 \
 aistore/cluster-minimal:latest
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
$ docker run -d \
 -p 51080:51080 \
 -v /disk0:/ais/disk0 \
 -v /disk1:/ais/disk1 \
 -v /some/disk2:/ais/disk2 \
 aistore/cluster-minimal:latest
```

> **IMPORTANT**: The mounted disk paths must resolve to _distinct_ and _disjoint_ file systems. Otherwise, the setup may be corrupted.


### <ins>Backend Provider Setup

> **IMPORTANT**: For both AWS or GCP usage, to ensure the cluster works properly with backend providers, it is _essential_ to pass the environment variable `AIS_BACKEND_PROVIDERS`, a space-separated list of support backend provides to be used, in your `docker run` command.

**AWS Backend**

The easiest way to pass your credentials to the AIS cluster is to mount a volume prepared with a [`config file`](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html) and provide `AWS_CONFIG_FILE`, the path to `config` in your `docker run` command:

```
$ docker run -d \
    -p 51080:51080 \
    -v <path_to_aws_config>:/path/to/config \
    -e AIS_CONFIG_FILE="/path/to/config" \
    -e AIS_BACKEND_PROVIDERS="aws" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```

Alternatively, it is possible to explicitly pass the credentials (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`) explicitly as [environment variables](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html) in your `docker run` command:

```
$ docker run -d \
    -p 51080:51080 \
    -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
    -e AWS_SECRET_ACCESS_KEY="wJalrXUtfdfUYBjdtEnFxEMEXAMPLE" \
    -e AWS_DEFAULT_REGION="us-west-2" \
    -e AIS_BACKEND_PROVIDERS="aws" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```

Once the container is running and the cluster is deployed, you can verify that your AWS buckets are accessible by the newly deployed AIS cluster:

```
$ AIS_ENDPOINT="http://localhost:51080" ais bucket ls

AWS Buckets (28)
  aws://sample-bucket-1
  aws://sample-bucket-2
  aws://sample-bucket-3
...
```

**GCP Backend**

> **WARNING**: The following section on `gcp` backend use needs _review_ and may be _outdated_.

The following command deploys a containerized AIS cluster with GCP by providing a volume with the `config file` and a path to the `config`:

```
$ docker run -d \
    -p 51080:51080 \
    -v <path_to_gcp_config>.json:/credentials/gcp.json \
    -e GOOGLE_APPLICATION_CREDENTIALS="/credentials/gcp.json" \
    -e AIS_BACKEND_PROVIDERS="gcp" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```

### <ins>Cloud Compute Deployment

Minimal deployments of AIS clusters can also be done on cloud compute instances, such as those provided by AWS EC2. Containerized deployment on a cloud compute cluster may be an appealing option for those wishing to continually run an AIS cluster in the background. 

 [`ssh`](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/AccessingInstancesLinux.html) into your EC2 instance and deploy an AIS cluster (as shown above). The following command creates a containerized AIS cluster with one mounted volume hosted locally:

```
docker run -d -p 51080:51080 -v /ais/sdf:/ais/disk0 aistore/cluster-minimal:latest
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

## Build and Upload (Dev)

It is also possible to [build a custom image](https://docs.docker.com/develop/develop-images/baseimages/) locally and upload it to selected registry (i.e. DockerHub, AWS, GitLab Container Registry):

```
$ ./build_image.sh <TAG>
$ ./upload_image.sh <REGISTRY_URL> <TAG>
```
