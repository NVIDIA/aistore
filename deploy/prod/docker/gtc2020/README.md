---
layout: post
title: GTC2020
permalink: deploy/prod/docker/gtc2020
redirect_from:
 - deploy/prod/docker/gtc2020/README.md/
---

# GTC2020 Infrastructure for AIS and WebDataset Demo

## Overview

## Build and upload

### AIS Binaries

If/when we need to rebuild the `aisnode` or `ais` binaries

```console
$ ./build_ais_binaries_containers.sh
```

will build (but not push to a repo) containers `ais-binaries:alpine` and `ais-binaries:ubuntu`, packaging the `aisnode` and `ais` binaries along with a CLI `config.json` for the DL client container.

To push these to Docker Hub:

```console
$ ./publish_ais_binaries.sh <tag>
```

will tag the local images as `gmaltby/ais-binaries:alpine-<tag>` and `gmaltby/ais-binaries:ubuntu-<tag>` and then push them to Docker Hub (as long as you have my ssh key etc). We push them to Docker Hub so that DLI can utilize these binaries in building containers without having to clone and build the ais repo.

### AIS Container

To build the AIS container for GTC:

```console
$ ./build_ais_container.sh <'local'|tag>
```

If the first argument is `local` then binaries from the local Docker image repo `ais-binaries:alpine` are used; otherwise binaries from `gmaltby/ais-binaries:alpine-<tag>` are used. A container image is built (but not further tagged or pushed) for `aisnode-gtc:latest` (local tag `latest` is always applied).

To push the container to Docker Hub:

```console
$ ./publish_ais_container.sh <tag>
```

This will further tag `aisnode-gtc:latest` as `gmaltby/aisnode-gtc:<tag>` and push it to Docker Hub. Avoid pushing tags such as `latest` to Docker Hub - rather use a dated tag, or a hash.

## Run

GTC will massage the following for Docker Compose; we describe how to run manually.

### Create a User Network

To be able to resolve container names in DNS we employ a user network - the default bridge does not support DNS. 

```console
$ docker network create gtc
```

### Run the AIS Container

This must be named `ais` (DL notebooks depend on the name) and be attached to the network created above.

```console
$ docker run --name ais --network gtc -it --rm gmaltby/aisnode-gtc:<tag>
```
choosing the published image, or point to a local one if desired. The container automatically runs a single proxy, single target AIS instance using the container root filesystem for data (so it's lost when the container exits!).

Since only the DL container is going to talk to AIS, there is no need to publish ports (all containers on a user network are open to one-another on all active ports).

### Run the DL Container (built externally)

```console
$ docker run --name dl --network gtc -it --rm -p 8888:8888 gmaltby/dltorch-gtc:<tag>
```

Connect to http://localhost:8888 to access the Jupyter notebook; the password is 'root'.
