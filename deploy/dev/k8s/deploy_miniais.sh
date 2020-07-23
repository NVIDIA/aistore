#!/bin/bash

# deletes any pre-existing minikube deployment
minikube delete
# we use docker as it is simple to use
minikube start --driver=docker

source ais_minikube_setup.sh

# waiting for setup changes to take place in kubernetes
sleep 20

export TARGET_CNT=1
export TEST_FSPATH_ROOT="/tmp/ais/"
export AIS_FS_PATHS="\"\":\"\""
export TEST_FSPATH_COUNT=3

export DOCKER_IMAGE=aistore/aisnode:dev
PROXY_PORT=8080
TARGET_PORT=8081
HOST_URL="http://$(minikube ip):${PROXY_PORT}"
export AIS_PRIMARY_URL=$HOST_URL
export IPV4LIST="$(minikube ip)"
export AIS_CLD_PROVIDER=""
export AIS_IS_PRIMARY=true

kubectl create secret generic aws-credentials --from-literal=dummy=dummy

echo "Starting kubernetes deployment..."

echo "Deploying proxy"
export PORT=$PROXY_PORT
export POD_NAME="ais-proxy"
envsubst < aisproxy_deployment.yml | kubectl apply -f -

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" pod ais-proxy

echo "Deploying target"
export PORT=$TARGET_PORT
export POD_NAME="ais-target"
envsubst < aistarget_deployment.yml | kubectl apply -f -


echo "List of running pods"
kubectl get pods -o wide

echo "Done"

echo "Please set the AIS_ENDPOINT for use of cli"
echo "export AIS_ENDPOINT=\"http://$(minikube ip):8080\""
