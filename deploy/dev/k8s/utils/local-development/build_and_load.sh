#!/bin/bash

# Build and load AIStore images for local development

set -e

echo "Building AIStore images for local development..."

make -C "../../prod/k8s/aisnode_container" IMAGE_TAG=local-development build
make -C "../../prod/k8s/aisinit_container" IMAGE_TAG=local-development build

echo "Loading images into $CLUSTER_TYPE cluster '$CLUSTER_NAME'..."

if [ "$CLUSTER_TYPE" = "kind" ]; then
    kind load docker-image aistorage/aisnode:local-development --name "$CLUSTER_NAME"
    kind load docker-image aistorage/ais-init:local-development --name "$CLUSTER_NAME"
elif [ "$CLUSTER_TYPE" = "minikube" ]; then
    minikube image load aistorage/aisnode:local-development -p "$CLUSTER_NAME"
    minikube image load aistorage/ais-init:local-development -p "$CLUSTER_NAME"
fi