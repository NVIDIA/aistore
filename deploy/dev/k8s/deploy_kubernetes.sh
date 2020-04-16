#!/bin/bash

GRAPHITE_PORT=2003
GRAPHITE_SERVER="52.41.234.112"
usage() {
    echo "Usage: $0"
    echo "   -a|--aws    : aws.env - AWS credentials"
    echo "   -m|--multi  : multiple-node Kubernetes cluster"
    echo "   -s|--single : single node Kubernetes cluster"
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

    -m|--multi)
        CLUSTER_CONFIG="multi"
        shift # past argument
        ;;

    -m|--multi)
        if [ "${CLUSTER_CONFIG}" != "multi" ]; then
            CLUSTER_CONFIG="single"
        fi
        shift # past argument
        ;;

    *)
        usage
        ;;
esac
done

create_local_repo() {
    if docker ps | grep registry:2 &> /dev/null; then
        echo "Local repository already exists ..."
        docker-compose down
    fi
    echo "Creating local repository and building image..."
    docker-compose up -d --force-recreate

    echo "Pushing to repository..."
    docker push localhost:5000/ais:v1
}


DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd -P)"

AISCONFDIR=/aisconfig
if [ ! -d "$AISCONFDIR" ]; then
    mkdir /aisconfig
    chmod 771 /aisconfig
fi

HOST="aisprimaryservice.default.svc.cluster.local"
PROXYID="ORIGINAL_PRIMARY"
PORT=8080
SERVICENAME="ais"
LOGDIR="/tmp/ais/log"
LOGLEVEL="3"
CONFDIR="/usr/nvidia"
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
if ! [[ "$PROXY_CNT" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$PROXY_CNT' must be at least 1"; exit 1
elif [ $PROXY_CNT -lt 1 ] ; then
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

FSPATHS=$fspath
TEST_FSPATH_COUNT=$testfspathcnt
LOCAL_AWS="/aisconfig/aws.env"
CLDPROVIDER="" # See deploy.sh for more informations about empty CLDPROVIDER
echo "Select:"
echo " 0: No 3rd party Cloud"
echo " 1: Amazon S3"
echo " 2: Google Cloud Storage"
echo " 3: Azure Cloud"
read cldprovider
if [ $cldprovider -eq 1 ]; then
    CLDPROVIDER="aws"

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

elif [ $cldprovider -eq 2 ]; then
  CLDPROVIDER="gcp"
  echo "" > ${LOCAL_AWS}

elif [ $cldprovider -eq 3 ]; then
  CLDPROVIDER="azure"
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

CONFFILE="ais.json"
CONFFILE_STATSD="statsd.conf"
CONFFILE_COLLECTD="collectd.conf"

export CONFDIR=/aisconfig
export PROXYURL="http://${HOST}:${PORT}"
export DOCKER_HOST_IP=$DOCKER_HOST_IP
export PROXY_LABEL=$PROXY_LABEL
export TARGET_LABEL=$TARGET_LABEL

echo $DIR
source $DIR/../local/aisnode_config.sh

create_local_repo $TARGET_CNT $CLDPROVIDER

# Deploying kubernetes cluster
echo Starting kubernetes deployment ..
#Create AIStore configmap to attach during runtime
echo Creating AIStore configMap
kubectl create configmap ais-config --from-file=./$CONFFILE
kubectl create configmap statsd-config --from-file=./$CONFFILE_STATSD
kubectl create configmap collectd-config --from-file=./$CONFFILE_COLLECTD


echo "Starting Primary Proxy Deployment ..."
envsubst < aisprimaryproxy_deployment.yml | kubectl apply -f -

#Give some room to breathe
echo "Waiting for primary proxy to start ..."
sleep 70

if (( $PROXY_CNT > 1 )); then
  echo "Starting Proxy Deployment"
  envsubst < aisproxy_deployment.yml | kubectl apply -f -
  PROXY_CNT=$((PROXY_CNT - 1))
  echo "Scaling proxies (${PROXY_CNT} more)"
  kubectl scale --replicas=$PROXY_CNT -f aisproxy_deployment.yml
fi

echo "Starting Target Deployment"
envsubst < aistarget_deployment.yml | kubectl create -f -

echo "Scaling targets"
kubectl scale --replicas=$TARGET_CNT -f aistarget_deployment.yml

echo "List of running pods"
kubectl get pods -o wide

rm $CONFFILE $CONFFILE_STATSD $CONFFILE_COLLECTD
echo "Done"
