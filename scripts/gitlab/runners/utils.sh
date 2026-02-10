#!/bin/bash

# Shared utilities for GitLab Runner setup scripts

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
  echo "Installing Docker..."
  apt-get update && apt-get install -y ca-certificates curl jq
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  
  echo \
    "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
    $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
    tee /etc/apt/sources.list.d/docker.list > /dev/null

  apt-get update && apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  systemctl status docker --no-pager

  if [[ -n "$SUDO_USER" ]]; then
    usermod -aG docker "$SUDO_USER"
  fi

  echo "Run 'newgrp docker' to allow root-less Docker access"
  echo "Docker installed successfully"
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
  echo "GitLab Runner installed successfully"
}

set_runner_concurrency() {
  echo "Setting runner concurrency to $CONCURRENCY..."
  sudo sed -i "s/^concurrent = .*/concurrent = $CONCURRENCY/" /etc/gitlab-runner/config.toml
  sudo systemctl restart gitlab-runner
}

configure_inotify_limits() {
  echo "Configuring inotify limits..."
  cat >> /etc/sysctl.conf <<EOF

fs.inotify.max_user_watches = 1048576
fs.inotify.max_user_instances = 1048576
EOF
  
  sysctl -p
  echo "inotify limits configured successfully"
}

setup_registry() {
  if [[ -n "$DATA_ROOT" ]]; then
    REGISTRY_DIR="$DATA_ROOT/registry"
  else
    REGISTRY_DIR="/var/lib/registry"
  fi

  echo "Setting up pull-through registry cache"
  sudo mkdir -p "$REGISTRY_DIR" /etc/containers/registries.conf.d
  sudo chown 1000:1000 "$REGISTRY_DIR"

  sudo tee /etc/containers/registries.conf.d/registries.conf <<EOF
unqualified-search-registries = ["docker.io"]

[[registry]]
prefix   = "docker.io"
location = "docker.io"

  [[registry.mirror]]
  location = "registry-proxy:$REGISTRY_PORT"
  insecure = true
EOF

  DOCKER_RUN_CMD=(
    docker run -d --name registry-proxy --restart=always -p 127.0.0.1:"$REGISTRY_PORT":5000
    --network ci-net
    -v "$REGISTRY_DIR:/var/lib/registry"
    -e REGISTRY_PROXY_REMOTEURL=https://registry-1.docker.io
  )

  if [[ -n "$REGISTRY_USERNAME" && -n "$REGISTRY_PASSWORD" ]]; then
    DOCKER_RUN_CMD+=(
      -e "REGISTRY_PROXY_USERNAME=$REGISTRY_USERNAME"
      -e "REGISTRY_PROXY_PASSWORD=$REGISTRY_PASSWORD"
    )
  fi

  DOCKER_RUN_CMD+=(registry:2)
  "${DOCKER_RUN_CMD[@]}"

  echo "Registry setup completed successfully!"
  echo "- Pull-through registry cache running on port $REGISTRY_PORT"
  echo "- Registry cache storage: $REGISTRY_DIR"
  echo "- Registry configuration: /etc/containers/registries.conf.d/registries.conf"
  if [[ -n "$REGISTRY_USERNAME" ]]; then
    echo "- Registry credentials configured for user: $REGISTRY_USERNAME"
  else
    echo "- Registry running without authentication (anonymous pulls only)"
  fi
}

