#!/bin/bash

set -e

ACTION=${1}

if [ "$ACTION" = "create" ]; then
    if [ "$CLUSTER_TYPE" = "kind" ]; then
        kind create cluster --name "$CLUSTER_NAME"
    elif [ "$CLUSTER_TYPE" = "minikube" ]; then
        minikube start -p "$CLUSTER_NAME"
    fi
    kubectl cluster-info
elif [ "$ACTION" = "delete" ]; then
    if [ "$CLUSTER_TYPE" = "kind" ]; then
        kind delete cluster --name "$CLUSTER_NAME"
    elif [ "$CLUSTER_TYPE" = "minikube" ]; then
        minikube delete -p "$CLUSTER_NAME"
    fi
fi 