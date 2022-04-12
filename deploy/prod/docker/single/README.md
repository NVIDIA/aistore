# Production-ready Standalone Docker

That is an absolutely minimal AIStore realization - the cluster consisting of a single storage target and a single proxy/gateway, all in one preconfigured ready-for-usage docker image.

Once `docker run`-deployed, the minimal cluster will be listening on the default AIS port `51080`.

## Run

`aistore/cluster-minimal:latest` Docker image is used to deploy the cluster.
Docker image can be started with attached volumes which will be used by the target to store the data.

**Important**: Currently assuming that all volumes will be mounted in `/ais/*` directory.

### Examples

#### Minimal setup

```console
$ docker run -d \
    -p 51080:51080 \
    -v $(mktemp -d):/ais/disk0 \
    aistore/cluster-minimal:latest
```

This starts AIS cluster with 1 disk (at least one disk is required!) that is mounted under temporary directory on the host.
The command exposes `51080` port, so it's possible to reach the cluster with `http://localhost:51080`.

To check the cluster status:
```console
$ AIS_ENDPOINT="http://localhost:51080" ais show cluster
PROXY                   MEM USED %  MEM AVAIL   UPTIME
proxy-0934deff64b7[P]   0.40%       7.78GiB     3m30s

TARGET              MEM USED %  MEM AVAIL   CAP USED %  CAP AVAIL   CPU USED %  REBALANCE   UPTIME
target-0934deff64b7 0.41%       7.78GiB     84%         8.950TiB    0.07%       -           3m30s

Summary:
 Proxies:	1 (0 - unelectable)
 Targets:	1
 Primary Proxy:	proxy-0934deff64b7
 Smap Version:	3
```

#### Multiple (static) disks

```console
$ docker run -d \
    -p 51080:51080 \
    -v /disk0:/ais/disk0 \
    -v /disk1:/ais/disk1 \
    -v /some/disk2:/ais/disk2 \
    aistore/cluster-minimal:latest
```

This starts AIS cluster with 3 disks (`/disk0`, `/disk1` and `/some/disk2`) with a sample configuration and exposes it on port `51080`.
Important note is that disk paths must resolve to distinct and disjoint filesystems, otherwise target will complain about the incorrect setup.

#### Backend provider

To make the cluster work properly with backend providers it is essential to set the environment variable `AIS_BACKEND_PROVIDERS`, a space separated list of supported provides, and pass the credentials to the docker image.
The easiest way to pass the credentials is to mount a volume and provide a path to config file as envvar:
 - AWS: `AWS_CONFIG_FILE`
 - GCP: `GOOGLE_APPLICATION_CREDENTIALS`

When using `aws` backend provider it is possible to explicitly pass the credentials with ([envvars](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)) :
 - `AWS_ACCESS_KEY_ID`
 - `AWS_SECRET_ACCESS_KEY`
 - `AWS_DEFAULT_REGION`


Start an AIS docker cluster with single disk and `aws` provider credentials in envvars:

```console
$ docker run -d \
    -p 51080:51080 \
    -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
    -e AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
    -e AWS_DEFAULT_REGION="us-west-2" \
    -e AIS_BACKEND_PROVIDERS="aws" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```


Start an AIS docker cluster with single disk and `gcp` provider credentials as a mounted volume:

```console
$ docker run -d \
    -p 51080:51080 \
    -v <path_to_gcp_config>.json:/credentials/gcp.json \
    -e GOOGLE_APPLICATION_CREDENTIALS="/credentials/gcp.json" \
    -e AIS_BACKEND_PROVIDERS="gcp" \
    -v /disk0:/ais/disk0 \
    aistore/cluster-minimal:latest
```


## (dev) Build and upload

It is possible to locally build an image and upload it to selected registry.

```console
$ ./build_image.sh <TAG>
$ ./upload_image.sh <REGISTRY_URL> <TAG>
```
