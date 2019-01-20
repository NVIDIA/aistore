#!/bin/bash

GRAPHITE_PORT=2003
GRAPHITE_SERVER="52.41.234.112"
usage() {
    echo "Usage: $0 -a <aws.env> [-s]"
    echo "   -a : aws.env - AWS credentials"
    echo "   -g : name or ip address of graphite server (default is $GRAPHITE_SERVER)"
    echo "   -p : port of graphite server (default is $GRAPHITE_PORT)"
    echo "   -s : scale the targets: add new targets to an already running cluster, or stop a given number of already running"
    echo
    exit 1;
}
environment="prod";
aws_env="";
os="ubuntu"
while getopts "a:g:p:s:" OPTION
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
   cp /tmp/ais_backup/* .
   sudo docker-compose -f $environment"_docker-compose.yml" up --no-recreate --scale aistarget=$scale -d aistarget
   rm aws.env
   rm ais.json
   sudo docker ps
   echo Done
   exit 0
fi

if [ -z "$aws_env" ]; then
   echo -a is a required parameter.Provide the path for aws.env file
   usage
fi

PROXYURL="http://aisproxy:8080"
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

echo Enter number of cache servers:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$servcount' is not a number"; exit 1
fi
echo Enter number of proxy servers:
read proxycount
if ! [[ "$proxycount" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$proxycount' must be at least 1"; exit 1
elif [ $proxycount -lt 1 ] ; then
  echo "Error: $proxycount is less than 1"; exit 1
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
   echo Enter filesystem info in comma seperated format ex: /tmp/ais1,/tmp/ais:
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

CLDPROVIDER="" # See deploy.sh for more informations about empty CLDPROVIDER
echo Select Cloud Provider:
echo  1: Amazon Cloud
echo  2: Google Cloud
echo  3: None
echo Enter your choice:
read cldprovider
if [ $cldprovider -eq 1 ]
then
  CLDPROVIDER="aws"
  cp $aws_env .
elif [ $cldprovider -eq 2 ]
then
  CLDPROVIDER="gcp"
fi

CONFFILE="ais.json"
c=0
CONFFILE_STATSD="statsd.conf"
CONFFILE_COLLECTD="collectd.conf"

source $DIR/../ais/setup/config.sh

#)  run the cluster
export CLDPROVIDER=$CLDPROVIDER
export MIRROR_ENABLED=false

echo Stoping running  cluster..
sudo docker-compose -f $environment"_docker-compose.yml" down
echo Building Image..
sudo docker-compose -f $environment"_docker-compose.yml" build
echo Starting Primary Proxy
sudo AIS_PRIMARYPROXY=TRUE docker-compose -f $environment"_docker-compose.yml" up -d aisproxy
echo Starting cluster ..
sudo docker-compose -f $environment"_docker-compose.yml" up -d --scale aistarget=$servcount --scale aisproxy=$proxycount --no-recreate
sleep 3
sudo docker ps
echo "Cleaning up files.."
mkdir -p /tmp/ais_backup
mv aws.env /tmp/ais_backup/
mv $CONFFILE /tmp/ais_backup/
mv $CONFFILE_STATSD /tmp/ais_backup/
mv $CONFFILE_COLLECTD /tmp/ais_backup/

echo done
