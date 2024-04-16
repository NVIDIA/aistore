#!/bin/bash

set -e

IMAGE_NAME=aistorage/aisnode-minikube

source ../utils.sh

echo "Enter number of storage targets:"
read -r TARGET_CNT
is_number ${TARGET_CNT}

echo "Enter number of proxies (gateway):"
read -r PROXY_CNT
is_number ${PROXY_CNT}
if [[ ${PROXY_CNT} -lt 1 ]]; then
  exit_error "${PROXY_CNT} is less than 1"
fi

source utils/parse_fsparams.sh
source utils/parse_cld.sh

export DOCKER_IMAGE="${IMAGE_NAME}:latest"

echo "Build and push to local registry: (y/n) ?"
read -r build
if [[ "$build" == "y" ]]; then
  export REGISTRY_URL="localhost:5000" && \
  ./utils/build_aisnode.sh
  export DOCKER_IMAGE="${REGISTRY_URL}/${DOCKER_IMAGE}"
fi


PRIMARY_PORT=8080

export HOSTNAME_LIST="$(minikube ip)"
export AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS}
export TARGET_CNT=${TARGET_CNT}
INSTANCE=0

echo "Enable HTTPS: (y/n)?"
read -r https
if [[ "$https" == "y" ]]; then
  source utils/create_certs.sh
  export HOST_URL="https://$(minikube ip):${PRIMARY_PORT}"
  export AIS_USE_HTTPS=true
  export AIS_SKIP_VERIFY_CRT=true
  export AIS_SERVER_CRT="/var/certs/tls.crt"
  export AIS_SERVER_KEY="/var/certs/tls.key"
  export PROTOCOL="HTTPS"
else
  export HOST_URL="http://$(minikube ip):${PRIMARY_PORT}"
  export AIS_USE_HTTPS=false
  export AIS_SKIP_VERIFY_CRT=false
  export AIS_SERVER_CRT=""
  export AIS_SERVER_KEY=""
  export PROTOCOL="HTTP"
fi

export AIS_PRIMARY_URL=$HOST_URL

# Deploying kubernetes cluster
echo "Starting kubernetes deployment..."

echo "Starting primary proxy deployment..."
for i in $(seq 0 $((PROXY_CNT-1))); do
  export POD_NAME="ais-proxy-${i}"
  export PORT=$((PRIMARY_PORT+i))
  export INSTANCE=${INSTANCE}
  export AIS_LOG_DIR="/tmp/ais/${INSTANCE}/log"
  (minikube ssh "sudo mkdir -p ${AIS_LOG_DIR}")
  ([[ $(kubectl get pods | grep -c "${POD_NAME}") -gt 0 ]] && kubectl delete pods ${POD_NAME}) || true
  envsubst < kube_templates/aisproxy_deployment.yml | kubectl apply -f -
  INSTANCE=$((INSTANCE+1))
done

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0

echo "Starting target deployment..."

for i in $(seq 0 $((TARGET_CNT-1))); do
  export POD_NAME="ais-target-${i}"
  export PORT=$((9090+i))
  export PORT_INTRA_CONTROL=$((9080+i))
  export PORT_INTRA_DATA=$((10080+i))
  export TARGET_POS_NUM=$i

  export INSTANCE=${INSTANCE}
  export AIS_LOG_DIR="/tmp/ais/${INSTANCE}/log"
  (minikube ssh "sudo mkdir -p ${AIS_LOG_DIR}")
  
  ([[ $(kubectl get pods | grep -c "${POD_NAME}") -gt 0 ]] && kubectl delete pods ${POD_NAME}) || true
  envsubst < kube_templates/aistarget_deployment.yml | kubectl create -f -
  INSTANCE=$((INSTANCE+1))
done

echo "Waiting for the targets to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pods -l type=aistarget

echo "List of running pods"
kubectl get pods -o wide

echo "Done."
echo ""
(cd ../../../  && make cli)
echo ""

export AIS_ENDPOINT=$HOST_URL

echo "Set the \"AIS_ENDPOINT\" for use of CLI:"
echo "export AIS_ENDPOINT=$HOST_URL"