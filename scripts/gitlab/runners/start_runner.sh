#!/bin/bash

# This script starts or restarts a Minikube cluster and registers a GitLab runner if necessary.

set -e

RUNNER_TOKEN=""
NODES=1
CONCURRENCY=1
DATA_ROOT=""
LPP_VERSION="v0.0.31"
TUNNEL=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
    --nodes)
      NODES="$2" && shift 2
      ;;
    --nodes=*)
      NODES="${1#*=}" && shift
      ;;
    --tunnel)
      TUNNEL="$2" && shift 2
      ;;
    --tunnel=*)
      TUNNEL="${1#*=}" && shift
      ;;
    --token)
      RUNNER_TOKEN="$2" && shift 2
      ;;
    --token=*)
      RUNNER_TOKEN="${1#*=}" && shift
      ;;
    --data-root)
      DATA_ROOT="$2" && shift 2
      ;;
    --data-root=*)
      DATA_ROOT="${1#*=}" && shift
      ;;
    --concurrency)
      CONCURRENCY="$2" && shift 2
      ;;
    --concurrency=*)
      CONCURRENCY="${1#*=}" && shift
      ;;
    --help|-h)
      echo "Usage: $0 [--nodes <number_of_nodes>] [--token <runner_token>] [--data-root <absolute_path>] [--concurrency <runner_concurrency>] [--tunnel <true|false>]"
      exit 0
      ;;
    *)
      echo "Unknown parameter: $1"
      echo "Usage: $0 [--nodes <number_of_nodes>] [--token <runner_token>] [--data-root <absolute_path>] [--concurrency <runner_concurrency>] [--tunnel <true|false>]"
      exit 1
      ;;
  esac
done

# Warn if no runner token
if [ -z "$RUNNER_TOKEN" ]; then
  echo "No runner token provided. No new runner will be registered."
fi

# Validate DATA_ROOT if provided
if [ -n "$DATA_ROOT" ] && [[ "$DATA_ROOT" != /* ]]; then
  echo "Error: --data-root must be an absolute path (e.g. /data)"
  exit 1
fi

GITLAB_HOST="https://gitlab-master.nvidia.com/"

# Requirements for each minikube node
MAX_CPU=16
MAX_RAM=32768
MIN_CPU=2
MIN_RAM=4096

# Reserved for host system
HOST_MEM=2000
HOST_CPU=2

NUM_CPU=$(nproc --all)
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')

# Reserve 2 cores for the host system, but do not exceed the max number of cores
MINIKUBE_CPU=$(((NUM_CPU-HOST_CPU) / NODES))
if [ "$MINIKUBE_CPU" -gt "$MAX_CPU" ]; then
  MINIKUBE_CPU=$MAX_CPU
elif [ "$MINIKUBE_CPU" -lt $MIN_CPU ]; then
  MINIKUBE_CPU=$MIN_CPU
fi

# Reserve 2000MB for the host system, but do not exceed the max amount of memory
MINIKUBE_MEMORY=$(((TOTAL_MEM - HOST_MEM) / NODES))
if [ "$MINIKUBE_MEMORY" -gt "$MAX_RAM" ]; then
  MINIKUBE_MEMORY=$MAX_RAM
elif [ "$MINIKUBE_MEMORY" -lt "$MIN_RAM" ]; then
  MINIKUBE_MEMORY=$MIN_RAM
fi
# Must be named this for minikube to pick it up, so we have a consistent config dir
export MINIKUBE_HOME=/var/local/minikube/.minikube

cleanup_minikube() {
  # Find all running minikube tunnel processes and get their PIDs
  PIDS=$(ps aux | grep '[m]inikube tunnel' | awk '{print $2}')

  # Check if any PIDs were found
  if [ -z "$PIDS" ]; then
      echo "No minikube tunnel process found."
  else
      # Kill the processes
      for PID in $PIDS; do
          echo "Terminating minikube tunnel process with PID: $PID"
          sudo kill -9 $PID
          if [ $? -eq 0 ]; then
              echo "Process $PID terminated."
          else
              echo "Failed to terminate process $PID."
          fi
      done
  fi
  rm -f minikube_tunnel.log
  minikube delete || true
}

# (Re)start Minikube
cleanup_minikube
minikube start --cpus=$MINIKUBE_CPU --memory=$MINIKUBE_MEMORY --nodes="$NODES"

# Required for hostPath mounts on multi-node clusters https://minikube.sigs.k8s.io/docs/tutorials/multi_node/
minikube addons enable volumesnapshots
minikube addons enable csi-hostpath-driver

# Install local path provisioner to allow dynamic local storage for state (used by k8s operator tests)
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/${LPP_VERSION}/deploy/local-path-storage.yaml

# Apply modified coredns config to allow for faster DNS updates
# Useful for tests where we create and destroy new services rapidly
kubectl replace -f coredns_config.yaml

# Register the runner if a token is provided
if [ -n "$RUNNER_TOKEN" ]; then
  echo "Registering GitLab Runner"
  sudo sed -i "1s/.*/concurrent = $CONCURRENCY/" /etc/gitlab-runner/config.toml
  REG_CMD=(
    sudo gitlab-runner register
    --non-interactive
    --url "$GITLAB_HOST"
    --token "$RUNNER_TOKEN"
    --name test-runner
    --executor kubernetes
    --kubernetes-host "$(minikube ip):8443"
    --kubernetes-image ubuntu:22.04
    --kubernetes-namespace default
    --kubernetes-cert-file "$MINIKUBE_HOME/profiles/minikube/apiserver.crt"
    --kubernetes-key-file "$MINIKUBE_HOME/profiles/minikube/apiserver.key"
    --kubernetes-ca-file "$MINIKUBE_HOME/ca.crt"
    --kubernetes-privileged=true
  )
  if [ -n "$DATA_ROOT" ]; then
    REG_CMD+=(
      --builds-dir "${DATA_ROOT}/builds"
      --cache-dir  "${DATA_ROOT}/cache"
    )
  fi
  "${REG_CMD[@]}"
fi

# Optionally open a Minikube tunnel indefinitely in the background
if [ "$TUNNEL" = "true" ]; then
  echo "Starting Minikube tunnel in the background..."
  nohup minikube tunnel > minikube_tunnel.log 2>&1 &
fi