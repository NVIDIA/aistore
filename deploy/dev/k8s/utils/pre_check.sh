#!/bin/bash
set +e
arch=$(uname -s | tr '[:upper:]' '[:lower:]')
# Installing minikube if not present
if ! command -v minikube &> /dev/null; then
  echo "minikube could not be found"
  echo "Fetching and installing minikube..."
  curl -Lo /tmp/minikube https://storage.googleapis.com/minikube/releases/latest/minikube-${arch}-amd64 \
  && chmod +x /tmp/minikube
  sudo mkdir -p /usr/local/bin/
  sudo install /tmp/minikube /usr/local/bin/
fi

if ! command -v kubectl &> /dev/null; then
  # Installing kubectl if not preset
  curl -LO "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/${arch}/amd64/kubectl" -o /tmp/kubectl
  chmod +x /tmp/kubectl
  sudo mv /tmp/kubectl /usr/local/bin/kubectl
fi

# Remove docker installed from snap
if command -v snap &> /dev/null; then
  sudo snap remove docker
fi

# Install docker from apt-get
if command -v apt-get &> /dev/null; then
  sudo apt-get install docker.io
fi

# The invoker of the parent script must not be root as `minikube`
# should not be run as root
if [[ $EUID -eq 0 ]]; then
  echo "This script must not be run as root"
  exit 1
fi


