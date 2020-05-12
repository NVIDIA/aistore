---
layout: post
title: SINGLE
permalink: deploy/prod/docker/single
redirect_from:
 - deploy/prod/docker/single/README.md/
---

# Production single standalone Docker

Production ready standalone AIStore docker starts up minimal AIS cluster.
The cluster consists of 1 target and 1 proxy.

The default exposed port from proxy is `51080`.

## Build and upload

It is possible to locally build an image and upload it to selected registry.

```console
$ ./build_image.sh <TAG>
$ ./upload_image.sh <REGISTRY_URL>
```

## Run

Docker image supports running with different cloud providers.
Cloud provider can be passed as a first argument.
Currently, supported cloud providers are: `""` (no provider), `"aws"` (Amazon), `"gcp"` (Google) and `"azure"` (Azure).

**Important**: Currently assuming that all volumes will be mounted in `/ais/*` directory.

### Examples

#### Minimal setup

```console
$ docker run \
    -p 51080:51080 \
    -v /disk0:/ais/disk0 \
    -v /disk1:/ais/disk1 \
    -v /some/disk2:/ais/disk2 \
    aistore/aistore-single-node ""
```

This starts AIS docker cluster with 3 disks (`/disk0`, `/disk1` and `/some/disk2`) with a sample configuration and exposes it on port 8080.
Important note is that disk paths must resolve to distinct and disjoint filesystems, otherwise target will complain about the incorrect setup.

#### Cloud provider

To make the cluster work properly with cloud providers it is essential to pass the credentials to the docker image.
The easiest way to pass the credentials is to mount a volume and provide a path to config file as envvar:
 - AWS: `AWS_CONFIG_FILE`
 - GCP: `GOOGLE_APPLICATION_CREDENTIALS`

When using `aws` cloud provider it is possible to explicitly pass the credentials with ([envvars](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-envvars.html)) :
 - `AWS_ACCESS_KEY_ID`
 - `AWS_SECRET_ACCESS_KEY`
 - `AWS_DEFAULT_REGION`


Start an AIS docker cluster with single disk, `aws` provider and credentials in envvars:

```console
$ docker run \
    -p 51080:51080 \
    -e AWS_ACCESS_KEY_ID="AKIAIOSFODNN7EXAMPLE" \
    -e AWS_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY" \
    -e AWS_DEFAULT_REGION="us-west-2" \
    -v /disk0:/ais/disk0 \
    aistore/aistore-single-node "aws"
```


Start an AIS docker cluster with single disk, `gcp` provider and credentials as a mounted volume:

```console
$ docker run \
    -p 51080:51080 \
    -v /home/user/Downloads/[GCP_CREDENTIALS].json:/credentials/[GCP_CREDENTIALS].json \
    -e GOOGLE_APPLICATION_CREDENTIALS="/credentials/[GCP_CREDENTIALS].json" \
    -v /disk0:/ais/disk0 \
    aistore/aistore-single-node "gcp"
```
