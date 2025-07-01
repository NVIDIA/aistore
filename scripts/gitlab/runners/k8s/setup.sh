#!/bin/bash

# This script installs Docker, Minikube, kubectl, and GitLab Runner if not already installed.

# Must be run as root or with sudo

set -e

SCRIPTS_DIR=$(dirname "$(realpath -s $"0")")
TMP_DOWNLOAD="$SCRIPTS_DIR/tmp_download"
DATA_ROOT=""
RUNNER_VERSION="18.1.1-1"
KUBECTL_VERSION="v1.33.0"

source "$SCRIPTS_DIR/../utils.sh"

# Create the directory if it doesn't already exist
if [ ! -d "$TMP_DOWNLOAD" ]; then
    mkdir -p "$TMP_DOWNLOAD"
fi
cd "$TMP_DOWNLOAD"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --data-root)
      DATA_ROOT="$2" && shift 2
      ;;
    --data-root=*)
      DATA_ROOT="${1#*=}" && shift
      ;;
    --help|-h)
      echo "Usage: $0 [--data-root <absolute_path>]"
      exit 0
      ;;
    *)
      echo "Unknown parameter: $1"
      echo "Usage: $0 [--data-root <absolute_path>]"
      exit 1
      ;;
  esac
done

# Validate and set DATA_ROOT
if [[ -n "$DATA_ROOT" ]]; then
  [[ "$DATA_ROOT" == /* ]] || { echo "Error: --data-root must be absolute."; exit 1; }
fi

install_minikube() {
  echo "Installing Minikube..."
  curl -Lo minikube https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  install minikube /usr/local/bin/minikube
  mkdir -p /var/local/minikube
  chmod 777 /var/local/minikube
}

install_kubectl() {
  echo "Installing kubectl $KUBECTL_VERSION..."
  curl -Lo kubectl https://dl.k8s.io/release/${KUBECTL_VERSION}/bin/linux/amd64/kubectl
  install -o root -g root -m0755 kubectl /usr/local/bin/kubectl
}

# Install Docker
if ! command -v docker &>/dev/null; then
  [[ -n "$DATA_ROOT" ]] && configure_docker_root_dir
  install_docker
fi

# Install Minikube
if ! command -v minikube &>/dev/null; then
  install_minikube
fi

# Install kubectl
if ! command -v kubectl &>/dev/null; then
  install_kubectl
fi

# Install GitLab Runner
if ! command -v gitlab-runner &>/dev/null; then
  install_runner
  [[ -n "$DATA_ROOT" ]] && configure_runner_dirs
fi

configure_inotify_limits

# Cleanup
cd "$SCRIPTS_DIR"
rm -rf "$TMP_DOWNLOAD"
