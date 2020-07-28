#!/bin/bash
set -e
source utils/pre_check.sh

# deletes any pre-existing minikube deployment
minikube delete

# we use docker as it is simple to use
minikube start --driver=docker

source utils/ais_minikube_setup.sh

echo "Start local registry: (y/n) ?"
read local_registry
if [[ "$local_registry" == "y" ]]; then
  source utils/minikube_registry.sh
fi
