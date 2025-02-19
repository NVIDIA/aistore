#!/bin/bash

set -e

# This script deploys AIS in an existing k8s cluster from within another node in the cluster
# Gitlab Kubernetes runners have access to kubectl but not minikube cmd line
# Each pod deployed will have its own clusterIP service to communicate without relying on host network 

source ../utils.sh

check_variable_set() {
    local var_name="$1"
    local var_value="${!var_name}"
    if [[ -z "$var_value" ]]; then
        echo "Error: Required variable '$var_name' is not set."
        exit 1
    fi
}

# List of required variables
required_vars=("AISNODE_IMAGE" "NUM_TARGET" "NUM_PROXY")

for var in "${required_vars[@]}"; do
    check_variable_set "$var"
done

is_number ${NUM_TARGET}
is_number ${NUM_PROXY}
if [[ ${NUM_PROXY} -lt 1 ]]; then
  exit_error "${NUM_PROXY} is less than 1"
fi

export TEST_FSPATH_COUNT=${FS_CNT:-4}
PRIMARY_PORT=8080
INSTANCE=0

if [[ -n $HTTPS ]]; then
  source utils/create_certs.sh
  export SCHEME="https://"
  export AIS_USE_HTTPS=true
  export AIS_SKIP_VERIFY_CRT=true
  export AIS_SERVER_CRT="/var/certs/tls.crt"
  export AIS_SERVER_KEY="/var/certs/tls.key"
  export PROTOCOL="HTTPS"
else
  export SCHEME="http://"
  export AIS_USE_HTTPS=false
  export AIS_SKIP_VERIFY_CRT=false
  export AIS_SERVER_CRT=""
  export AIS_SERVER_KEY=""
  export PROTOCOL="HTTP"
fi

export AIS_LOG_DIR="/tmp/ais/log"
export AIS_PRIMARY_HOST="ais-proxy-0.default.svc.cluster.local"
export AIS_PRIMARY_URL="${SCHEME}${AIS_PRIMARY_HOST}:${PRIMARY_PORT}"

if [[ $AIS_BACKEND_PROVIDERS == *"gcp"* ]]; then
  echo "Creating GCP credentials secret"
  kubectl delete secret gcp-creds || true
  kubectl create secret generic gcp-creds \
    --from-file=creds.json=$GOOGLE_APPLICATION_CREDENTIALS
fi

if [[ $AIS_BACKEND_PROVIDERS == *"aws"* ]]; then
  echo "Creating AWS credentials secrets"
  kubectl delete secret aws-credentials || true
  kubectl create secret generic aws-credentials \
    --from-literal=AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID \
    --from-literal=AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
    --from-literal=AWS_DEFAULT_REGION=$AWS_DEFAULT_REGION
fi

if [[ $AIS_BACKEND_PROVIDERS == *"oci"* ]]; then
  echo "Creating OCI credentials secret"
  kubectl delete secret oci-credentials || true
  kubectl create secret generic oci-credentials \
    --from-file=OCI_PRIVATE_KEY=$ORACLE_PRIVATE_KEY \
    --from-literal=OCI_TENANCY_OCID=$OCI_TENANCY_OCID \
    --from-literal=OCI_USER_OCID=$OCI_USER_OCID \
    --from-literal=OCI_REGION=$OCI_REGION \
    --from-literal=OCI_FINGERPRINT=$OCI_FINGERPRINT \
    --from-literal=OCI_COMPARTMENT_OCID=$OCI_COMPARTMENT_OCID
fi

# Necessary in case another run with different spec is aborted 
echo "Cleaning up any previous deployment"
source utils/cleanup_k8s_ci.sh

echo "Starting AIS deployment..."
echo "Starting primary proxy deployment..."
for i in $(seq 0 $((NUM_PROXY-1))); do
  export POD_NAME="ais-proxy-${i}"
  export PORT=$((PRIMARY_PORT+i))
  export INSTANCE=${INSTANCE}
  # Use a clusterIP service for each pod for communication without host network
  export HOSTNAME_LIST="$POD_NAME.default.svc.cluster.local"
  envsubst < kube_templates/ci_proxy.yml | kubectl delete -f - || true 
  envsubst < kube_templates/ci_proxy.yml | kubectl apply -f -
  INSTANCE=$((INSTANCE+1))
done

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pod ais-proxy-0

echo "Starting target deployment..."

INSTANCE=0
for i in $(seq 0 $((NUM_TARGET-1))); do
  export POD_NAME="ais-target-${i}"
  export PORT=$((9090+i))
  export PORT_INTRA_CONTROL=$((9080+i))
  export PORT_INTRA_DATA=$((10080+i))
  export TARGET_POS_NUM=$i
  export INSTANCE=${INSTANCE}
  export HOSTNAME_LIST="$POD_NAME.default.svc.cluster.local"
  
  envsubst < kube_templates/ci_target.yml | kubectl delete -f - || true
  envsubst < kube_templates/ci_target.yml | kubectl create -f -
  INSTANCE=$((INSTANCE+1))
done

echo "Waiting for the targets to be ready..."
kubectl wait --for="condition=ready" --timeout=2m pods -l type=ais-target

echo "List of running pods"
kubectl get pods -o wide
