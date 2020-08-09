#!/bin/bash
set -e
if command -v minikube &> /dev/null; then
  echo "Stopping AIS Clusters, deleting minikube"
  minikube delete
fi
