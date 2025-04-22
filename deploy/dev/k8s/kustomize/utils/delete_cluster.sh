#!/bin/bash

set -e

if [ "$CLUSTER_TYPE" = "kind" ]; then
    kind delete cluster
elif [ "$CLUSTER_TYPE" = "minikube" ]; then
    minikube delete
fi