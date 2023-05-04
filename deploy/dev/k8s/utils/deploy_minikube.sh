#!/bin/bash

set -e

source utils/pre_check.sh

# Delete any pre-existing minikube deployment.
minikube delete

# We use docker as it is simple to use.
# It's run in subshell to not "eat" the input.
minikube_cpu=${MINIKUBE_CPU:-4}
minikube_memory=${MINIKUBE_MEMORY:-9000}
(minikube start --driver=docker --cpus ${minikube_cpu} --memory ${minikube_memory})

source utils/ais_minikube_setup.sh

# Check if we should set up a local registry
if [ -z "${USE_LOCAL_REGISTRY}" ]; then
  echo "Start local registry: (y/n) ?"
  read -r local_registry
  if [[ "$local_registry" == "y" ]]; then
    export USE_LOCAL_REGISTRY=true
  fi
fi

if [ "$USE_LOCAL_REGISTRY" = true ]; then
  source utils/minikube_registry.sh
fi


# Check if we should deploy prometheus metrics
if [ -z "${DEPLOY_METRICS}" ]; then
  echo "Deploy metrics collection (Prometheus operator): (y/n) ?"
  read -r metrics
  if [[ "$metrics" == "y" ]]; then
    export DEPLOY_METRICS=true
  fi
fi

if [ "$DEPLOY_METRICS" = true ]; then
  source utils/enable_metrics.sh
fi
