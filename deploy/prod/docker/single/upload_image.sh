#!/usr/bin/env bash

REGISTRY_URL=${1:-docker.io}

DOCKER_ORG=$(shell docker info 2>/dev/null | sed '/Username:/!d;s/.* //')
if [[ -n ${DOCKER_ORG} ]]; then
  echo "WARNING: No docker user found using results from whoami"
  DOCKER_ORG=$(shell whoami)
fi

docker push ${REGISTRY_URL}/aistore/aistore-single-node:latest
