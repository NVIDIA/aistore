#!/bin/bash

set -e

export MODE="debug"

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

export DOCKER_IMAGE="aistorage/aisnode-minikube:latest"
echo "Build and push to local registry: (y/n) ?"
read -r build
if [[ "$build" == "y" ]]; then
  echo "Building image with mode=${MODE}..."
  export DOCKER_IMAGE="localhost:5000/aisnode-minikube:latest"
  docker build ./../../../ --force-rm -t ${DOCKER_IMAGE} --build-arg MODE="${MODE}" -f Dockerfile
  docker push ${DOCKER_IMAGE}
fi

PRIMARY_PORT=8080
HOST_URL="http://$(minikube ip):${PRIMARY_PORT}"

export AIS_PRIMARY_URL=$HOST_URL
export HOSTNAME_LIST="$(minikube ip)"
export AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS}
export TARGET_CNT=${TARGET_CNT}

# Deploying kubernetes cluster
echo "Starting kubernetes deployment..."

echo "Starting primary proxy deployment..."
for i in $(seq 0 $((PROXY_CNT-1))); do
  export POD_NAME="ais-proxy-${i}"
  export PORT=$((PRIMARY_PORT+i))
  if [ $PORT -eq $PRIMARY_PORT ]; then
    export AIS_IS_PRIMARY=true
  else
    export AIS_IS_PRIMARY=false
  fi
  ([[ $(kubectl get pods | grep -c "${POD_NAME}") -gt 0 ]] && kubectl delete pods ${POD_NAME}) || true
  envsubst < kube_templates/aisproxy_deployment.yml | kubectl apply -f -
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
  # Prepare directory for target's hostpath
  (minikube ssh "sudo mkdir -p /tmp/${TARGET_POS_NUM}")

  ([[ $(kubectl get pods | grep -c "${POD_NAME}") -gt 0 ]] && kubectl delete pods ${POD_NAME}) || true
  envsubst < kube_templates/aistarget_deployment.yml | kubectl create -f -
done

echo "Waiting for the targets to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pods -l type=aistarget

echo "List of running pods"
kubectl get pods -o wide

echo "Done."
echo ""
echo "Set the \"AIS_ENDPOINT\" for use of CLI:"
echo "export AIS_ENDPOINT=\"http://$(minikube ip):8080\""
