---
layout: post
title: AISNODE_CONTAINER
permalink: deploy/prod/k8s/aisnode_container
redirect_from:
 - deploy/prod/k8s/aisnode_container/README.md/
---

# Building AIStore Containers

The `aisnode` container delivers the main AIStore binary and runtime support and can be updated often.

Correspondingly, you will need a container repository name and tag - the defaults point to a private repository server on `quay.io`.

1. Have Go available in your `PATH` or at `/usr/local/go/bin` and at the appropriate version - see function `check_go_version` in `build_ais_binary.sh`
1. Clone this repo
1. `cd deploy/prod/k8s/aisnode_container`
1. To update AIStore: `REPO_AISNODE=repo.name/ais/aisnode REPO_TAG_AISNODE=20200504 make aisnode` will build, tag and push a container image named `repo.name/ais/aisnode:20200504`
