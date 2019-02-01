#!/bin/bash

GRAPHITE_PORT=2003
GRAPHITE_SERVER="52.41.234.112"
usage() {
    echo "Usage: $0"
    echo "   -a : aws.env - AWS credentials"
    echo "   -g : name or ip address of graphite server (default is $GRAPHITE_SERVER)"
    echo "   -p : port of graphite server (default is $GRAPHITE_PORT)"
    echo
    exit 1;
}
aws_env="";
os="ubuntu"
while getopts "a:g:p:" OPTION
do
    case $OPTION in
    a)
        aws_env=${OPTARG}
        ;;

    g)
        GRAPHITE_SERVER=${OPTARG}
        ;;

    p)
        GRAPHITE_PORT=${OPTARG}
        ;;

    *)
        usage
        ;;
    esac
done

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ "${PWD##*/}" != "docker" ]; then
    cd $DIR
fi

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
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TESTFSPATHROOT="/tmp/ais/"

echo Enter number of target servers:
read TARGET_CNT
if ! [[ "$TARGET_CNT" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$TARGET_CNT' is not a number"; exit 1
fi
START=0
END=$TARGET_CNT


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

echo $FSPATHS
FSPATHS=$fspath
TESTFSPATHCOUNT=$testfspathcnt
LOCAL_AWS="/aisconfig/aws.env"
CLDPROVIDER="" # See deploy.sh for more informations about empty CLDPROVIDER
echo Select Cloud Provider:
echo  1: Amazon Cloud
echo  2: Google Cloud
echo  3: None
echo Enter your choice:
read cldprovider
if [ $cldprovider -eq 1 ]; then
    CLDPROVIDER="aws"

    echo "Enter the location of your AWS configuration and credentials files:"
    echo "Note: No input will result in using the default aws dir (~/.aws/)"
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
fi

if kubectl get secrets | grep aws > /dev/null 2>&1; then
    kubectl delete secret aws-credentials
fi
kubectl create secret generic aws-credentials --from-file=$LOCAL_AWS

CONFFILE="ais.json"
c=0
CONFFILE_STATSD="statsd.conf"
CONFFILE_COLLECTD="collectd.conf"
MIRROR_UTIL_THRESH=20
MIRROR_ENABLED=false
IOSTAT_TIME="2s"
USE_HTTPS=false
NON_ELECTABLE=false
AUTHENABLED=false

export CONFDIR=/aisconfig
export PROXYURL="http://${HOST}:${PORT}"
export MIRROR_UTIL_THRESH=$MIRROR_UTIL_THRESH
export IOSTAT_TIME=$IOSTAT_TIME
export USE_HTTPS=$USE_HTTPS
export NON_ELECTABLE=$NON_ELECTABLE
export AUTHENABLED=$AUTHENABLED

source $DIR/../../../ais/setup/config.sh

#1) create/update/delete kubctl configmap
#)  run the cluster

# Deploying kubernetes cluster
echo Starting kubernetes deployment ..
#Create AIStore configmap to attach during runtime
echo Creating AIStore configMap
kubectl create configmap ais-config --from-file=./$CONFFILE
kubectl create configmap statsd-config --from-file=./$CONFFILE_STATSD
kubectl create configmap collectd-config --from-file=./$CONFFILE_COLLECTD

echo Starting Primary Proxy Deployment
kubectl create -f aisprimaryproxy_deployment.yml

#Give some room to breathe
echo "Waiting for proxy to start ..."
sleep 50

echo "Starting Target Deployment"
kubectl create -f aistarget_deployment.yml

echo "Scaling targets"
kubectl scale --replicas=$TARGET_CNT -f aistarget_deployment.yml

echo "List of running pods"
kubectl get pods -o wide

rm $CONFFILE $CONFFILE_STATSD $CONFFILE_COLLECTD

echo "Dones"