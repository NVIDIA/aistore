#!/bin/bash

source utils/ais_minikube_setup.sh

# Default values and environment variables
export AIS_FS_PATHS=""
export TEST_FSPATH_COUNT=4
REGISTRY_URL="${REGISTRY_URL:-docker.io}"
IMAGE_TAG="latest"
export DOCKER_IMAGE="${REGISTRY_URL}/aistorage/aisnode-minikube:${IMAGE_TAG}"


PRIMARY_PORT=8080
HOST_URL="http://$(minikube ip):${PRIMARY_PORT}"

export AIS_PRIMARY_URL=$HOST_URL
export HOSTNAME_LIST="$(minikube ip)"
export AIS_BACKEND_PROVIDERS=""
export TARGET_CNT=1

export POD_NAME="ais-proxy-0"
export PORT=$PRIMARY_PORT
export AIS_IS_PRIMARY=true

([[ $(kubectl get pods | grep -c "${POD_NAME}") -gt 0 ]] && kubectl delete pods ${POD_NAME}) || true
envsubst < kube_templates/aisproxy_deployment.yml | kubectl apply -f -

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0

echo "Starting target deployment..."

export POD_NAME="ais-target-0"
export PORT=9090
export PORT_INTRA_CONTROL=9080
export PORT_INTRA_DATA=10080
export TARGET_POS_NUM=1

(minikube ssh "sudo mkdir -p /tmp/${TARGET_POS_NUM}")

# Delete and apply target deployment
([[ $(kubectl get pods | grep -c "${POD_NAME}") -gt 0 ]] && kubectl delete pods ${POD_NAME}) || true
envsubst < kube_templates/aistarget_deployment.yml | kubectl create -f -

echo "Waiting for the targets to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pods -l type=aistarget

# Display a list of running pods
echo "List of running pods"
kubectl get pods -o wide

echo "Done."
echo ""

# Build the CLI in the appropriate directory
(cd ../../../ && make cli)
echo ""

# Set AIS_ENDPOINT for use with the CLI
export AIS_ENDPOINT="http://$(minikube ip):8080"
echo "Set the \"AIS_ENDPOINT\" for use of CLI:"
echo "export AIS_ENDPOINT=\"$AIS_ENDPOINT\""