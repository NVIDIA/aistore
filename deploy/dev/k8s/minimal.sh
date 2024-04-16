#!/bin/bash

source utils/ais_minikube_setup.sh

# Default values and environment variables
export AIS_FS_PATHS=""
export TEST_FSPATH_COUNT=4
REGISTRY_URL="${REGISTRY_URL:-docker.io}"
IMAGE_TAG="latest"

if [ -n "$1" ]; then
  HTTPS="$1"
fi

IMAGE_NAME=aistorage/aisnode-minikube
export DOCKER_IMAGE="${REGISTRY_URL}/${IMAGE_NAME}:${IMAGE_TAG}"

PRIMARY_PORT=8080

if [ -n "${HTTPS}" ] && [ "${HTTPS}" == "true" ]; then
  source utils/create_certs.sh
  HOST_URL="https://$(minikube ip):${PRIMARY_PORT}"
  export AIS_USE_HTTPS=true
  export AIS_SKIP_VERIFY_CRT=true
  export AIS_SERVER_CRT="/var/certs/tls.crt"
  export AIS_SERVER_KEY="/var/certs/tls.key"
  export PROTOCOL="HTTPS"
else
  HOST_URL="http://$(minikube ip):${PRIMARY_PORT}"
  export AIS_USE_HTTPS=false
  export AIS_SKIP_VERIFY_CRT=false
  export AIS_SERVER_CRT=""
  export AIS_SERVER_KEY=""
  export PROTOCOL="HTTP"
fi

export AIS_PRIMARY_URL=$HOST_URL
export HOSTNAME_LIST="$(minikube ip)"
export AIS_BACKEND_PROVIDERS=""
export TARGET_CNT=1

export POD_NAME="ais-proxy-0"
export PORT=$PRIMARY_PORT
export INSTANCE=0

export AIS_LOG_DIR="/tmp/ais/${INSTANCE}/log"
(minikube ssh "sudo mkdir -p ${AIS_LOG_DIR}")

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
export INSTANCE=1

export AIS_LOG_DIR="/tmp/ais/${INSTANCE}/log"
(minikube ssh "sudo mkdir -p ${AIS_LOG_DIR}")

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

# Set AIS_ENDPOINT for use with the CLI
export AIS_ENDPOINT=$HOST_URL
echo "Set the \"AIS_ENDPOINT\" for use of CLI:"
echo "export AIS_ENDPOINT=\"$AIS_ENDPOINT\""
