#!/bin/bash

set -e

if [ "$CLUSTER_TYPE" = "kind" ]; then
    kind create cluster
elif [ "$CLUSTER_TYPE" = "minikube" ]; then
    minikube start
fi

kubectl cluster-info