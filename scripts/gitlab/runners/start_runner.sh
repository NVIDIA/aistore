#!/bin/bash

# This script starts or restarts minikube with a specific amount of resources, 
# registers a gitlab runner if necessary, then opens a minikube tunnel 

# Check if the RUNNER_TOKEN was provided
RUNNER_TOKEN=$1
if [ -z "$RUNNER_TOKEN" ]; then
  echo "No runner token provided. No new runner will be registered."
fi

# Install required packages and make sure the gitlab runner system service is running
sudo ./setup.sh

GITLAB_HOST="https://gitlab-master.nvidia.com/"
NUM_CPU=$(nproc --all)
TOTAL_MEM=$(free -m | awk '/^Mem:/{print $2}')
MINIKUBE_CPU=$(($NUM_CPU-2))
MINIKUBE_MEMORY=$(($TOTAL_MEM-2000))
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
  rm minikube_tunnel.log
  minikube stop
  minikube delete
}

# (Re)Start minikube
cleanup_minikube
minikube start --cpus=$MINIKUBE_CPU --memory=$MINIKUBE_MEMORY

# Apply RBAC to allow the default service account admin privileges
kubectl apply -f minikube_rbac.yaml

# Apply modified coredns config to allow for faster DNS updates
# Useful for tests where we create and destroy new services rapidly
kubectl replace -f coredns_config.yaml

# Register the runner in short-lived config container if a runner token is provided
if [ -n "$RUNNER_TOKEN" ]; then
echo "Running gitlab-runner register to create config with new token"
  sudo gitlab-runner register \
  --non-interactive \
  --url "$GITLAB_HOST" \
  --token "$RUNNER_TOKEN" \
  --name test-runner \
  --executor kubernetes \
  --kubernetes-host "$(minikube ip):8443" \
  --kubernetes-image ubuntu:22.04 \
  --kubernetes-namespace default \
  --kubernetes-cert-file "$MINIKUBE_HOME/profiles/minikube/apiserver.crt" \
  --kubernetes-key-file "$MINIKUBE_HOME/profiles/minikube/apiserver.key" \
  --kubernetes-ca-file "$MINIKUBE_HOME/ca.crt"
fi

# Open a minikube tunnel indefinitely
nohup minikube tunnel > minikube_tunnel.log 2>&1 &