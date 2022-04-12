#!/bin/bash

REGISTRY_URL=${1:-docker.io}
TAG=${2:-aistore/cluster-minimal:latest}

docker push ${REGISTRY_URL}/${TAG}
