#!/bin/bash

set -e

source utils/pre_check.sh

# Delete any pre-existing minikube deployment.
minikube delete

# We use docker as it is simple to use.
# It's run in subshell to not "eat" the input.
(minikube start --driver=docker)

source utils/ais_minikube_setup.sh

echo "Start local registry: (y/n) ?"
read -r local_registry
if [[ "$local_registry" == "y" ]]; then
  source utils/minikube_registry.sh
fi
