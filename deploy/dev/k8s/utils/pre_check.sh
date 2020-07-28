#!/bin/bash
set +e
arch=$(uname -s | tr '[:upper:]' '[:lower:]')
# Installing minikube if not present
if ! command -v minikube &> /dev/null; then
  echo "minikube could not be found"
  echo "Fetching and installing minikube..."
  curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-${arch}-amd64 \
  && chmod +x minikube
  sudo mkdir -p /usr/local/bin/
  sudo install minikube /usr/local/bin/
fi

if ! command -v kubectl &> /dev/null; then
  # Installing kubectl if not preset
  curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/${arch}/amd64/kubectl"
  chmod +x ./kubectl
  sudo mv ./kubectl /usr/local/bin/kubectl
fi

echo "Checking kubectl default sa account..."
kubectl get sa default >/dev/null 2>&1
if [ $? -ne 0 ]; then
  kubectl create sa default
fi

# The invoker of the parent script must not be root as `minikube`
# should not be run as root
if [[ $EUID -eq 0 ]]; then
  echo "This script must not be run as root"
  exit 1
fi


