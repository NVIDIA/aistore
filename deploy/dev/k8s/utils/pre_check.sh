#!/bin/bash
set +e
os=$(uname -s | tr '[:upper:]' '[:lower:]')
# Installing minikube if not present
install_minikube='true'
if command -v minikube &> /dev/null; then
  MINIKUBE_VER=$(minikube version --short 2>&1 | cut -d'v' -f2)
  if ! [[ "$MINIKUBE_VER" < "1.20.0" ]] ; then
    install_minikube='false'
  fi
fi
if [[ "$install_minikube" == "true" ]]; then
  echo "minikube >= v1.20.0 could not be found"
  echo "Fetching and installing the latest minikube ..."
  curl -Lo /tmp/minikube https://storage.googleapis.com/minikube/releases/latest/minikube-${os}-amd64 \
  && chmod +x /tmp/minikube
  sudo mkdir -p /usr/local/bin/
  sudo install /tmp/minikube /usr/local/bin/
fi

# Installing kubectl if not present
if ! command -v kubectl &> /dev/null; then
  curl -Lo /tmp/kubectl "https://storage.googleapis.com/kubernetes-release/release/$(curl -s https://storage.googleapis.com/kubernetes-release/release/stable.txt)/bin/${os}/amd64/kubectl"
  chmod +x /tmp/kubectl
  sudo mv /tmp/kubectl /usr/local/bin/kubectl
fi

# The invoker of the parent script must not be root as `minikube`
# should not be run as root
if [[ $EUID -eq 0 ]]; then
  echo "This script must not be run as root"
  exit 1
fi


