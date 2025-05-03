#!/bin/bash

# This script installs Docker, Minikube, kubectl, and GitLab Runner if not already installed.

# Must be run as root or with sudo

set -e

SCRIPTS_DIR=$(dirname "$(realpath -s $"0")")
TMP_DOWNLOAD="$SCRIPTS_DIR/tmp_download"
DATA_ROOT=""
RUNNER_VERSION="17.11.0-1"
KUBECTL_VERSION="v1.33.0"

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

configure_docker_root_dir() {
  echo "Configuring Docker data root: $DATA_ROOT/docker"
  mkdir -p "$DATA_ROOT/docker" /etc/docker
  cat > /etc/docker/daemon.json <<EOF
{
  "data-root": "$DATA_ROOT/docker"
}
EOF
}

install_docker() {
  # Add Docker's official GPG key
  apt-get update && apt-get install -y ca-certificates curl jq
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  
  # Add the repository to Apt sources
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null

  # Install the Docker packages
  apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  
  # Check if the Docker system service is running
  systemctl status docker --no-pager

  # Assuming this script is run as sudo, add the calling user to the docker group
  usermod -aG docker "$SUDO_USER"

  echo "Docker installed"
  echo "Log out and back in or run 'newgrp docker' to allow root-less docker access (required for minikube). Then re-run start_runner.sh"
}

configure_runner_dirs() {
  echo "Configuring GitLab Runner storage: $DATA_ROOT/gitlab-runner"
  mkdir -p "$DATA_ROOT/gitlab-runner/builds" "$DATA_ROOT/gitlab-runner/cache" "/home/gitlab-runner"
  chown -R gitlab-runner:gitlab-runner "$DATA_ROOT/gitlab-runner"
}

install_runner() {
  echo "Installing GitLab Runner..."
  curl -L https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh | bash
  apt-get install -y gitlab-runner="$RUNNER_VERSION"
  mkdir -p /home/gitlab-runner
  chown -R gitlab-runner:gitlab-runner /home/gitlab-runner
  systemctl enable gitlab-runner
  systemctl restart gitlab-runner
  systemctl status gitlab-runner --no-pager
  echo "GitLab Runner installed."
}

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

# Cleanup
cd "$SCRIPTS_DIR"
rm -rf "$TMP_DOWNLOAD"
