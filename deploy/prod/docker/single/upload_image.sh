#!/bin/bash

REGISTRY_URL=${1:-docker.io}
TAG=${2:-aistore/aistore:latest-minimal}

docker push ${REGISTRY_URL}/${TAG}
