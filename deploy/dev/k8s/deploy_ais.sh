#!/bin/bash

usage() {
    echo "Usage: $0"
    echo "   -a|--aws    : aws.env - AWS credentials"
    echo "   -h|--host   : IP of your local Docker registry host"
    echo
    exit 1;
}
aws_env="";


for i in "$@"; do
case $i in
    -a=*|--aws=*)
        aws_env="${i#*=}"
        shift # past argument=value
        ;;

    -h=*|--host=*)
        DOCKER_HOST_IP="${i#*=}"
        shift
        ;;

    *)
        usage
        ;;
esac
done

echo Enter number of target servers:
read TARGET_CNT
if ! [[ "$TARGET_CNT" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$TARGET_CNT' is not a number"; exit 1
fi

echo "Enter number of proxy servers:"
read PROXY_CNT
if ! [[ "$PROXY_CNT" =~ ^[0-9]+$ ]]; then
  echo "Error: '$PROXY_CNT' must be at least 1"; exit 1
elif [[ $PROXY_CNT -lt 1 ]]; then
  echo "Error: $PROXY_CNT is less than 1"; exit 1
fi


source parse_fsparams.sh
source parse_cld.sh
source local_build.sh

PRIMARY_PORT=8080
HOST_URL="http://$(minikube ip):${PRIMARY_PORT}"

export AIS_PRIMARY_URL=$HOST_URL
export IPV4LIST="$(minikube ip)"
export AIS_CLD_PROVIDER=${AIS_CLD_PROVIDER}
export TARGET_CNT=${TARGET_CNT}

# Deploying kubernetes cluster
echo "Starting kubernetes deployment..."


echo "Starting primary proxy deployment..."
for i in $(seq 0 $(($PROXY_CNT-1))); do
  export POD_NAME="ais-proxy-${i}"
  export PORT=$(($PRIMARY_PORT+$i))
  if [ $PORT -eq $PRIMARY_PORT ]; then
    export AIS_IS_PRIMARY=true
  else
    export AIS_IS_PRIMARY=false
  fi
  envsubst < aisproxy_deployment.yml | kubectl apply -f -
done

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" pod ais-proxy-0

echo "Starting target deployment..."

for i in $(seq 0 $(($TARGET_CNT-1))); do
  export POD_NAME="ais-target-${i}"
  export PORT=$((9090+$i))
  envsubst < aistarget_deployment.yml | kubectl create -f -
done

echo "List of running pods"
kubectl get pods -o wide

echo "Done"
