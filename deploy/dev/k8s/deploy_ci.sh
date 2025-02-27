#!/bin/bash
#
# Deploy AIS (AIStore) on a Kubernetes cluster (e.g., Minikube, KinD).
# 
# This script requires the environment variables AISNODE_IMAGE, AISINIT_IMAGE, 
# NUM_PROXY, and NUM_TARGET, as well as at least as many nodes as the maximum 
# of NUM_PROXY or NUM_TARGET.
# 

set -e

source ../utils.sh

NODE_COUNT=$(kubectl get nodes --no-headers | wc -l)
MAX_NODES_NEEDED=$(( NUM_PROXY > NUM_TARGET ? NUM_PROXY : NUM_TARGET ))
if [ "$NODE_COUNT" -lt "$MAX_NODES_NEEDED" ]; then
    echo "The cluster requires at least $MAX_NODES_NEEDED nodes, but only $NODE_COUNT are available."
    exit 1
fi

echo "Cleaning up any previous deployment..."
source utils/cleanup_k8s_ci.sh

if [[ -n $HTTPS ]]; then
  echo "Configuring HTTPS..."
  source utils/create_certs.sh
  SCHEME="https://"
  export AIS_USE_HTTPS=true
  export AIS_SKIP_VERIFY_CRT=true
  export AIS_SERVER_CRT="/var/certs/tls.crt"
  export AIS_SERVER_KEY="/var/certs/tls.key"
  export PROTOCOL="HTTPS"
else
  SCHEME="http://"
  export AIS_USE_HTTPS=false
  export AIS_SKIP_VERIFY_CRT=false
  export AIS_SERVER_CRT=""
  export AIS_SERVER_KEY=""
  export PROTOCOL="HTTP"
fi

if [[ $AIS_BACKEND_PROVIDERS == *"gcp"* ]]; then
  echo "Creating GCP credentials secret..."
  kubectl delete secret gcp-creds || true
  kubectl create secret generic gcp-creds \
    --from-file=creds.json=$GOOGLE_APPLICATION_CREDENTIALS
fi

if [[ $AIS_BACKEND_PROVIDERS == *"aws"* ]]; then
  echo "Creating AWS credentials secrets..."
  kubectl delete secret aws-credentials || true
  kubectl create secret generic aws-credentials \
    --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    --from-literal=AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION
fi

if [[ $AIS_BACKEND_PROVIDERS == *"oci"* ]]; then
  echo "Creating OCI credentials secret..."
  kubectl delete secret oci-credentials || true
  kubectl create secret generic oci-credentials \
    --from-file=OCI_PRIVATE_KEY=$ORACLE_PRIVATE_KEY \
    --from-literal=OCI_TENANCY_OCID=$OCI_TENANCY_OCID \
    --from-literal=OCI_USER_OCID=$OCI_USER_OCID \
    --from-literal=OCI_REGION=$OCI_REGION \
    --from-literal=OCI_FINGERPRINT=$OCI_FINGERPRINT \
    --from-literal=OCI_COMPARTMENT_OCID=$OCI_COMPARTMENT_OCID
fi

export AIS_BACKEND_PROVIDERS=$(make_backend_conf)

echo "Starting AIS K8s deployment..."

export INSTANCE=0

echo "Deploying ${NUM_PROXY} proxies..."

export PORT=8080
export PORT_INTRA_CONTROL=9080
export PORT_INTRA_DATA=10080
export TEST_FSPATH_COUNT=0
export INSTANCE=0
export CONFIGMAP_NAME="ci-proxy-cm"
export AIS_LOG_DIR="/tmp/ais/log/proxy"
export AIS_PRIMARY_URL="${SCHEME}ais-proxy-0.ais-proxy.default.svc.cluster.local:${PORT}"

envsubst < kube_templates/ci_configmap.yml | kubectl apply -f -
envsubst < kube_templates/ci_proxy.yml | kubectl apply -f -

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0

echo "Deploying ${NUM_TARGET} targets..."

export PORT=9090
export PORT_INTRA_CONTROL=9080
export PORT_INTRA_DATA=10080
export TEST_FSPATH_COUNT=${FS_CNT:-4}
export INSTANCE=0
export AIS_LOG_DIR="/tmp/ais/log/target"
export CONFIGMAP_NAME="ci-target-cm"

envsubst < kube_templates/ci_configmap.yml | kubectl apply -f -
envsubst < kube_templates/ci_target.yml | kubectl create -f -

echo "Waiting for the targets to be ready..."
kubectl rollout status statefulset/ais-target --timeout=2m

echo "Completed deployment of ${NUM_PROXY} proxies and ${NUM_TARGET} targets:"

kubectl get pods -o wide

export EXTERNAL_IP=$(kubectl get pod ais-proxy-0 -o jsonpath='{.status.hostIP}')
export EXTERNAL_PORT=$(kubectl get pod ais-proxy-0 -o jsonpath='{.spec.containers[0].ports[0].hostPort}')

echo "For connectivity outside the cluster: export AIS_ENDPOINT=${SCHEME}${EXTERNAL_IP}:${EXTERNAL_PORT}."
echo "For connectivity within the cluster: export AIS_ENDPOINT=${AIS_PRIMARY_URL}."