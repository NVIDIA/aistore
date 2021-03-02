#!/bin/bash

set -e

source utils/pre_check.sh

# Delete any pre-existing minikube deployment.
minikube delete

# We use docker as it is simple to use.
# It's run in subshell to not "eat" the input.
minikube_cpu=${MINIKUBE_CPU:-4}
minikube_memory=${MINIKUBE_MEMORY:-16384}
(minikube start --driver=docker --cpus ${minikube_cpu} --memory ${minikube_memory})

source utils/ais_minikube_setup.sh

echo "Start local registry: (y/n) ?"
read -r local_registry
if [[ "$local_registry" == "y" ]]; then
  source utils/minikube_registry.sh
fi

echo "Deploy metrics collection (Prometheus operator): (y/n) ?"
read -r metrics
if [[ "$metrics" == "y" ]]; then
  # See https://github.com/prometheus-operator/kube-prometheus/
  tmpdir=$(mktemp -d)
  pushd $tmpdir
  git clone https://github.com/prometheus-operator/kube-prometheus.git
  # Kubectl returns error when resources already exist...
  kubectl apply -f kube-prometheus/manifests/setup
  kubectl apply -f kube-prometheus/manifests
  popd
  rm -rf $tmpdir
fi
