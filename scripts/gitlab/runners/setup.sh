
#!/bin/bash
# Combination of steps from https://confluence.nvidia.com/pages/viewpage.action?pageId=2565826909#id-%F0%9F%A6%8AGitLabRunnerSetup-ContainerRuntime 
# Run as root
set -e

SCRIPTS_DIR=$(dirname $(realpath -s $0))
TMP_DOWNLOAD="$SCRIPTS_DIR/tmp_download"

SYSBOX_VER=v0.6.4
SYSBOX_PKG=sysbox-ce_0.6.4-0.linux_amd64.deb
RUNNER_VERSION=16.9.1-1

# Create the directory if it doesn't already exist
if [ ! -d "$TMP_DOWNLOAD" ]; then
    mkdir -p "$TMP_DOWNLOAD"
fi
cd $TMP_DOWNLOAD

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
  usermod -aG docker $SUDO_USER

  echo "Docker installed"
  echo "Log out and back in or run 'newgrp docker' to allow root-less docker access (required for minikube). Then re-run start_runner.sh"
}

confirm_docker_rm() {
    # Call with a prompt string or use a default
    read -r -p "${1:-Are you sure you want to remove all Docker containers? [y/N]} " response
    case "$response" in
        [yY][eE][sS]|[yY]) 
            true
            ;;
        *)
            false
            ;;
    esac
}

install_sysbox() {
  echo "Installing Sysbox Docker runtime"
  # Download the latest Sysbox .deb package on the GitHub releases page (https://github.com/nestybox/sysbox/releases). We want the Linux x86-64 (AMD64) variant.
  wget https://downloads.nestybox.com/sysbox/releases/$SYSBOX_VER/$SYSBOX_PKG

  if confirm_docker_rm; then
    echo "Removing all Docker containers..."
    docker rm $(docker ps -a -q) -f || true
  else
      echo "Canceled setup script due to user request."
      exit 1
  fi

  # Install Sysbox.
  apt install ./$SYSBOX_PKG
  
  # Check if the Sysbox system service is running.
  systemctl status sysbox --no-pager

  # Delete the Sysbox .deb package.
  rm $SYSBOX_PKG

  # If docker daemon json is empty, create a json config object
  if [ ! -s /etc/docker/daemon.json ]; then
    echo '{}' | tee /etc/docker/daemon.json
  fi

  # Set Docker Engine's default container runtime to sysbox-runc. We can't read from and write to the same file: https://github.com/jqlang/jq/issues/2152
  jq '. + {
      "default-runtime": "sysbox-runc",
      "runtimes": {
        "sysbox-runc": {
            "path": "/usr/bin/sysbox-runc"
        }
      }
  }' /etc/docker/daemon.json | tee /etc/docker/daemon-staging.json && mv -f /etc/docker/daemon-staging.json /etc/docker/daemon.json

  # Restart the Docker Engine system service.
  systemctl restart docker
}

install_runner() {
  echo "Installing Gitlab Runner"
  #Add the official GitLab repository.
  curl -L "https://packages.gitlab.com/install/repositories/runner/gitlab-runner/script.deb.sh" | sudo bash
  # Install GitLab Runner.
  apt install gitlab-runner=$RUNNER_VERSION
}

start_runner() {
  # Enable the GitLab Runner system service in system mode (runs as a newly-created gitlab-runner user with root privileges).
  systemctl enable gitlab-runner
  # Check if the GitLab Runner system service is running.
  systemctl status gitlab-runner --no-pager
}

# Install docker if needed
if ! docker info > /dev/null 2>&1; then
  echo "Installing Docker"
  install_docker
fi

defaultRuntime=$(docker info --format '{{.DefaultRuntime}}')

# TODO: Restore later if we need docker available in runners
# if [ "$defaultRuntime" != "sysbox-runc" ]; then
#   echo "Installing sysbox and setting as default Docker runtime"
#   install_sysbox
# fi

# Install minikube if needed
if [ ! -f /usr/local/bin/minikube ]; then
  echo "Installing minikube"
  curl -LO https://storage.googleapis.com/minikube/releases/latest/minikube-linux-amd64
  install minikube-linux-amd64 /usr/local/bin/minikube
  mkdir -p /var/local/minikube -m 777
fi

# Install kubectl if needed
if [ ! -f /usr/local/bin/kubectl ]; then
  echo "Installing kubectl"
  curl -LO https://dl.k8s.io/release/v1.29.2/bin/linux/amd64/kubectl
  install -o root -g root -m 0755 kubectl /usr/local/bin/kubectl
fi

# Install gitlab-runner if needed
if ! dpkg -l | grep -qw gitlab-runner; then
  install_runner
fi

# Start gitlab-runner service if needed
if ! systemctl is-active --quiet gitlab-runner; then
  start_runner
fi

cd $SCRIPTS_DIR
rm -rf "$TMP_DOWNLOAD"