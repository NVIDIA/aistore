#!/bin/bash

GRAPHITE_PORT=2003
GRAPHITE_SERVER="52.41.234.112"
usage() {
    echo "Usage: $0"
    echo "   -a|--aws    : aws.env - AWS credentials"
    echo "   -g          : name or ip address of graphite server (default is $GRAPHITE_SERVER)"
    echo "   -p          : port of graphite server (default is $GRAPHITE_PORT)"
    echo "   -h|--host   : IP of your local Docker registry host"
    echo
    exit 1;
}
aws_env="";
os="ubuntu"

for i in "$@"
do
case $i in
    -a=*|--aws=*)
        aws_env="${i#*=}"
        shift # past argument=value
        ;;

    -g)
        GRAPHITE_SERVER="${i#*=}"
        shift # past argument=value
        ;;

    -p)
        GRAPHITE_PORT="${i#*=}"
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

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"
PRIMARY_PORT=8080
HOST_URL="http://$(minikube ip):${PRIMARY_PORT}"
DOCKER_HOST_IP=""
CLUSTER_CONFIG=""

###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TEST_FSPATH_ROOT="/tmp/ais/"

PROXY_LABEL=""
TARGET_LABEL=""
echo "Enter 's' for single node cluster configuration or 'm' for multi-node configuration.."
read CLUSTER_CONFIG
if [ "$CLUSTER_CONFIG" = "s" ]; then
    PROXY_LABEL="ais"
    TARGET_LABEL="ais"
elif [ $CLUSTER_CONFIG = 'm' ] ; then
    PROXY_LABEL="ais-proxy"
    TARGET_LABEL="ais-target"
else
    echo "Invalid cluster configuration."
    usage
fi


echo Enter number of target servers:
read TARGET_CNT
if ! [[ "$TARGET_CNT" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$TARGET_CNT' is not a number"; exit 1
fi
START=0
END=$TARGET_CNT

echo "Enter number of proxy servers:"
read PROXY_CNT
if ! [[ "$PROXY_CNT" =~ ^[0-9]+$ ]]; then
  echo "Error: '$PROXY_CNT' must be at least 1"; exit 1
elif [[ $PROXY_CNT -lt 1 ]]; then
  echo "Error: $PROXY_CNT is less than 1"; exit 1
fi
START=0
END=$PROXY_CNT


testfspathcnt=0
fspath="\"\":\"\""
echo Select
echo  1: Local cache directories
echo  2: Filesystems
echo Enter your cache choice:
read cachesource
if [ $cachesource -eq 1 ]
then
   echo Enter number of local cache directories:
   read testfspathcnt
   if ! [[ "$testfspathcnt" =~ ^[0-9]+$ ]] ; then
       echo "Error: '$testfspathcnt' is not a number"; exit 1
   fi
fi
if [ $cachesource -eq 2 ]
then
   echo Enter filesystem info in comma separated format ex: /tmp/ais1,/tmp/ais:
   read fsinfo
   fspath=""
   IFS=',' read -r -a array <<< "$fsinfo"
   for element in "${array[@]}"
   do
      fspath="$fspath,\"$element\" : \"\" "
   done
   fspath=${fspath#","}
fi

export AIS_FS_PATHS=$fspath
export TEST_FSPATH_COUNT=$testfspathcnt
echo $AIS_FS_PATHS
LOCAL_AWS="/tmp/aws.env"
AIS_CLD_PROVIDER="" # See deploy.sh for more informations about empty AIS_CLD_PROVIDER
echo "Select:"
echo " 0: No 3rd party Cloud"
echo " 1: Amazon S3"
echo " 2: Google Cloud Storage"
echo " 3: Azure Cloud"
read cldprovider
if [ $cldprovider -eq 1 ]; then
    AIS_CLD_PROVIDER="aws"

    echo "Enter the location of your AWS configuration and credentials files:"
    echo "Note: No input will result in using the default AWS dir (~/.aws/)"
    read aws_env

    if [ -z "$aws_env" ]; then
        aws_env="~/.aws/"
    fi
    # to get proper tilde expansion
    aws_env="${aws_env/#\~/$HOME}"
    temp_file="$aws_env/credentials"
    if [ -f $"$temp_file" ]; then
        cp $"$temp_file"  ${LOCAL_AWS}
    else
        echo "No AWS credentials file found in specified directory. Exiting..."
        exit 1
    fi

    # By default, the region field is found in the aws config file.
    # Sometimes it is found in the credentials file.
    if [ $(cat "$temp_file" | grep -c "region") -eq 0 ]; then
        temp_file="$aws_env/config"
        if [ -f $"$temp_file" ] && [ $(cat $"$temp_file" | grep -c "region") -gt 0 ]; then
            grep region "$temp_file" >> ${LOCAL_AWS}
        else
            echo "No region config field found in aws directory. Exiting..."
            exit 1
        fi
    fi

    sed -i 's/\[default\]//g' ${LOCAL_AWS}
    sed -i 's/ = /=/g' ${LOCAL_AWS}
    sed -i 's/aws_access_key_id/AWS_ACCESS_KEY_ID/g' ${LOCAL_AWS}
    sed -i 's/aws_secret_access_key/AWS_SECRET_ACCESS_KEY/g' ${LOCAL_AWS}
    sed -i 's/region/AWS_DEFAULT_REGION/g' ${LOCAL_AWS}

elif [[ $cldprovider -eq 2 ]]; then
  AIS_CLD_PROVIDER="gcp"
  echo "" > ${LOCAL_AWS}
elif [[ $cldprovider -eq 3 ]]; then
  AIS_CLD_PROVIDER="azure"
  echo "" > ${LOCAL_AWS}
else
  AIS_CLD_PROVIDER=""
  echo "" > ${LOCAL_AWS}
fi

if [ -z "$DOCKER_HOST_IP" ]; then
    echo "Enter the internal IP of the host that runs the local Docker registry"
    echo "Note: No input will result in using your current system's internal IP ($(hostname -I | head -n1 | awk '{print $1;}'))"
    read DOCKER_HOST_IP

    if [ -z "$DOCKER_HOST_IP" ]; then
        DOCKER_HOST_IP=$(hostname -I | head -n1 | awk '{print $1;}')
    fi
fi

if kubectl get secrets | grep aws > /dev/null 2>&1; then
    kubectl delete secret aws-credentials
fi
kubectl create secret generic aws-credentials --from-file=$LOCAL_AWS

export AIS_PRIMARY_URL=$HOST_URL
export DOCKER_HOST_IP=$DOCKER_HOST_IP
export PROXY_LABEL=$PROXY_LABEL
export TARGET_LABEL=$TARGET_LABEL
export IPV4LIST="$(minikube ip)"
export AIS_CLD_PROVIDER=${AIS_CLD_PROVIDER}
export TARGET_CNT=${TARGET_CNT}

echo $DIR

# Deploying kubernetes cluster
echo "Starting kubernetes deployment..."

echo "Building image..."
docker-compose up -d --force-recreate --build --remove-orphans
echo "Pushing to repository..."
docker push localhost:5000/ais:v1

echo "Starting primary proxy deployment..."
for i in $(seq 0 $(($PROXY_CNT-1))); do
  export ID=$i
  export PORT=$(($PRIMARY_PORT+$i))
  if [ $PORT -eq $PRIMARY_PORT ]; then
    export AIS_IS_PRIMARY=true
  else
    export AIS_IS_PRIMARY=false
  fi
  envsubst < aisproxy_deployment.yml | kubectl apply -f -
done

echo "Waiting for the primary proxy to be ready..."
kubectl wait --for="condition=ready" pod aisproxy-0

echo "Starting target deployment..."

for i in $(seq 0 $(($TARGET_CNT-1)));do
  export ID=$i
  export PORT=$((9090+$i))
  envsubst < aistarget_deployment.yml | kubectl create -f -
done

echo "List of running pods"
kubectl get pods -o wide

echo "Done"
