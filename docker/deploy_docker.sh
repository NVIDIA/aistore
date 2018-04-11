#!/bin/bash

GRAPHITE_SERVER="52.41.234.112"
usage() {
    echo "Usage: $0 [-e dev|prod|k8s] -a <aws.env> [-s]"
    echo "   -a : aws.env - AWS credentials"
    echo "   -e : dev|prod - deployment environment (default is dev[elopment])"
    echo "   -g : name or ip address of graphite server (default is $GRAPHITE_SERVER)"
    echo "   -s : scale the targets: add new targets to an already running cluster, or stop a given number of already running"
    echo
    exit 1;
}
environment="dev";
aws_env="";
os="ubuntu"
while getopts "e:a:o:s:" OPTION
do
    case $OPTION in
    a)
        aws_env=${OPTARG}
        ;;

    e)
        environment=${OPTARG}
        ;;

    g)
        GRAPHITE_SERVER=${OPTARG}
        ;;

    s)
        scale=${OPTARG}
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


if [ ! -z "$scale" ]; then
   echo "Scaling cluster "
   cp /tmp/dfc_backup/* .
   sudo docker-compose -f $environment"_docker-compose.yml" up --no-recreate --scale dfctarget=$scale -d dfctarget
   rm aws.env
   rm dfc.json
   sudo docker ps
   echo Done
   exit 0
fi

if [ -z "$aws_env" ]; then
   echo -a is a required parameter.Provide the path for aws.env file
   usage
fi

PROXYURL="http://dfcproxy:8080"
PORT=8080
SERVICENAME="dfc"
LOGDIR="/tmp/dfc/log"
LOGLEVEL="3"
CONFDIR="/usr/nvidia"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TESTFSPATHROOT="/tmp/dfc/"

echo Enter number of cache servers:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$servcount' is not a number"; exit 1
fi
START=0
END=$servcount


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
   echo Enter filesystem info in comma seperated format ex: /tmp/dfc1,/tmp/dfc:
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

echo Select Cloud Provider:
echo  1: Amazon Cloud
echo  2: Google Cloud
echo Enter your choice:
read cldprovider
if [ $cldprovider -eq 1 ]
then
  CLDPROVIDER="aws"
  cp $aws_env .
  if [ "$environment" == "k8s" ]; then
    # creating aws credential files
    rm -rf credentials
    cat $aws_env >> credentials
    sed -i '1 i\[default]' credentials
    sed -i 's/AWS_ACCESS_KEY_ID/aws_access_key_id/g' credentials
    sed -i 's/AWS_SECRET_ACCESS_KEY/aws_secret_access_key/g' credentials
    sed -i 's/AWS_DEFAULT_REGION/region/g' credentials
    kubectl delete secret generic aws-credentials
    kubectl create secret generic aws-credentials --from-file=./credentials
  fi
elif [ $cldprovider -eq 2 ]
then
  CLDPROVIDER="gcp"
else
  echo "Error: '$cldprovider' is not a valid input, can be either 1 or 2"; exit 1
fi

CONFFILE="dfc.json"
c=0
CONFFILE_STATSD="statsd.conf"
CONFFILE_COLLECTD="collectd.conf"
source $DIR/../dfc/setup/config.sh

#1) create/update/delete kubctl configmap
#)  run the cluster

if [ "$environment" == "k8s" ]; then
    # Deploying kubernetes cluster
    echo Starting kubernetes deployment ..
    #Create DFC configmap to attach during runtime
    echo Creating DFC configMap
    kubectl delete configmap dfc-config
    kubectl delete configmap collectd-config
    kubectl delete configmap statsd-config
    kubectl create configmap dfc-config --from-file=dfc.json
    kubectl create configmap statsd-config --from-file=statsd.conf
    kubectl create configmap collectd-config --from-file=collectd.conf

    echo Stopping DFC cluster
    kubectl delete -f dfctarget_deployment.yml
    kubectl delete -f dfcproxy_deployment.yml

    echo Starting Proxy Deployment
    kubectl create -f dfcproxy_deployment.yml

    echo Wating for proxy to start ....
    sleep 100

    echo Starting Target Deployment
    kubectl create -f dfctarget_deployment.yml

    echo Scaling targets
    kubectl scale --replicas=$servcount -f dfctarget_deployment.yml

    echo List of running pods
    kubectl get pods -o wide

else
    echo Stoping running  cluster..
    sudo docker-compose -f $environment"_docker-compose.yml" down
    echo Building Image..
    sudo docker-compose -f $environment"_docker-compose.yml" build
    echo Starting cluster ..
    sudo docker-compose -f $environment"_docker-compose.yml" up -d --scale dfctarget=$servcount
    sleep 3
    sudo docker ps
    echo "Cleaning up files.."
    mkdir -p /tmp/dfc_backup
    mv aws.env /tmp/dfc_backup/
    mv $CONFFILE /tmp/dfc_backup/
    mv $CONFFILE_STATSD /tmp/dfc_backup/
    mv $CONFFILE_COLLECTD /tmp/dfc_backup/
fi
echo done
