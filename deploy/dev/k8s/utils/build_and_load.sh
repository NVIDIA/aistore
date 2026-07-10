#!/bin/bash

# Build and load AIStore images into a local Kubernetes cluster

set -e

if [ -z "$IMAGE_TAG" ]; then
  echo "Error: IMAGE_TAG must be set" >&2
  exit 1
fi

echo "Building AIStore images with tag '$IMAGE_TAG'..."

make -C "../../prod/k8s/aisnode_container" IMAGE_TAG="$IMAGE_TAG" build
make -C "../../prod/k8s/aisinit_container" IMAGE_TAG="$IMAGE_TAG" build

echo "Loading images into $CLUSTER_TYPE cluster '$CLUSTER_NAME'..."

if [ "$CLUSTER_TYPE" = "kind" ]; then
    kind load docker-image "aistorage/aisnode:$IMAGE_TAG" --name "$CLUSTER_NAME"
    kind load docker-image "aistorage/ais-init:$IMAGE_TAG" --name "$CLUSTER_NAME"
elif [ "$CLUSTER_TYPE" = "minikube" ]; then
    minikube image load "aistorage/aisnode:$IMAGE_TAG" -p "$CLUSTER_NAME"
    minikube image load "aistorage/ais-init:$IMAGE_TAG" -p "$CLUSTER_NAME"
else
    echo "Error: unsupported CLUSTER_TYPE '$CLUSTER_TYPE'" >&2
    exit 1
fi
