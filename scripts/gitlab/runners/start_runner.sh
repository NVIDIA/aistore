#!/bin/bash

# This script registers a GitLab runner if necessary.

set -e

SCRIPTS_DIR=$(dirname "$(realpath -s $"0")")

# Source shared utilities
source "$SCRIPTS_DIR/utils.sh"

RUNNER_TOKEN=""
CONCURRENCY=1
DATA_ROOT=""
GITLAB_HOST="https://gitlab-master.nvidia.com/"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
  case "$1" in
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
      echo "Usage: $0 --token <runner_token> [--data-root <absolute_path>] [--concurrency <number>]"
      exit 0
      ;;
    *)
      echo "Unknown parameter: $1"
      echo "Usage: $0 --token <runner_token> [--data-root <absolute_path>] [--concurrency <number>]"
      exit 1
      ;;
  esac
done

# Require runner token
if [ -z "$RUNNER_TOKEN" ]; then
  echo "Error: --token is required"
  echo "Usage: $0 --token <runner_token> [--data-root <absolute_path>] [--concurrency <number>]"
  exit 1
fi

# Validate DATA_ROOT if provided
if [ -n "$DATA_ROOT" ] && [[ "$DATA_ROOT" != /* ]]; then
  echo "Error: --data-root must be an absolute path (e.g. /data)"
  exit 1
fi

# Register the runner
echo "Registering GitLab Runner"
REG_CMD=(
  sudo gitlab-runner register
  --non-interactive
  --url "$GITLAB_HOST"
  --token "$RUNNER_TOKEN"
  --executor docker
  --docker-image ubuntu:24.04
  --docker-network-mode "ci-net"
  --docker-privileged=true
  --docker-volumes "/etc/containers/registries.conf.d:/etc/containers/registries.conf.d:ro"
)
if [ -n "$DATA_ROOT" ]; then
  REG_CMD+=(
    --builds-dir "${DATA_ROOT}/gitlab-runner/builds"
    --cache-dir  "${DATA_ROOT}/gitlab-runner/cache"
  )
fi
"${REG_CMD[@]}"
set_runner_concurrency

echo -e "\nSetup completed successfully!"
echo "- Runner registered successfully"
echo "- Concurrency set to: $CONCURRENCY" 