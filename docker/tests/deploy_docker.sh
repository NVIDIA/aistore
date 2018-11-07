#!/bin/bash

usage() {
    echo "Usage: $0 [-h] [-a <aws.env>] [-p <proxy count>] [-t <target count>] [-l <Testing FSPaths count>] [-s </fs1,/fs2>] [-m]"
    echo "   -a : aws.env - AWS credentials (if -a is not set then GCP is used)"
    echo "   -p : 0 - the number of proxies"
    echo "   -t : 0 - the number of targets"
    echo "   -s : '' - the list of filesystems separated with comma (if it is empty then -l must be used)"
    echo "   -l : 0 - the number of TestingFSPaths (used only if -s is empty)"
    echo "   -m : use multiple networks"
    echo "   -h : show usage"
    echo
    exit 1;
}

USE_AWS=0
PROXY_CNT=0
TARGET_CNT=0
USE_FS=0
FS_LIST=""
TESTFS_CNT=0
network="single"
LOCAL_AWS="./aws.env"

aws_env="";
os="ubuntu"
while getopts "a:p:t:f:l:m" OPTION
do
    case $OPTION in
    a)
        aws_env=${OPTARG}
        USE_AWS=1
        ;;

    p)
        PROXY_CNT=${OPTARG}
        ;;

    t)
        TARGET_CNT=${OPTARG}
        ;;

    s)
        FS_LIST=${OPTARG}
        USE_FS=1
        ;;

    l)
        TESTFS_CNT=${OPTARG}
        ;;

    m)
        network="multi"
        ;;

    h)
        usage
        ;;

    *)
        usage
        ;;
    esac
done

PWD=$(pwd)
DIR=$(dirname "${BASH_SOURCE[0]}")
DIR="${PWD}/${DIR}"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ "${PWD##*/}" != "docker" ]; then
    cd $DIR
fi

PUB_NET="172.50.0"
PUB_SUBNET="${PUB_NET}.0/24"
INT_NET="172.51.0"
INT_SUBNET="${INT_NET}.0/24"

IPV4LIST=""
IPV4LIST_INTRA_CONTROL=""

for i in `seq 2 $(( $TARGET_CNT + $PROXY_CNT + 1 ))`; do
    IPV4LIST="${IPV4LIST}${PUB_NET}.$i,"
done
if [ "$IPV4LIST" != "" ]; then
    IPV4LIST=${IPV4LIST::-1} # remove last ","
fi

if [ "$network" = "multi" ]; then
    for i in `seq 2 $(( $TARGET_CNT + $PROXY_CNT + 1 ))`; do
        IPV4LIST_INTRA_CONTROL="${IPV4LIST_INTRA_CONTROL}${INT_NET}.$i,"
    done
    IPV4LIST_INTRA_CONTROL=${IPV4LIST_INTRA_CONTROL::-1} # remove last ","
fi

PORT=8080
PORT_INTRA=9080
PROXYURL="http://${PUB_NET}.2:${PORT}"

echo "Public network: ${PUB_SUBNET}"
echo "Internal network: ${INT_SUBNET}"
echo "Network type: ${network}"
export PUB_SUBNET=$PUB_SUBNET
export INT_SUBNET=$INT_SUBNET

SERVICENAME="dfc"
LOGDIR="/tmp/dfc/log"
LOGLEVEL="3"
USE_HTTPS="false"
NON_ELECTABLE="false"
AUTHENABLED="false"
CONFFILE_STATSD="statsd.conf"
CONFFILE_COLLECTD="collectd.conf"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TESTFSPATHROOT="/tmp/dfc/"

comp_base="docker-compose"
composer_file="${comp_base}.singlenet.yml"
if [ "$network" = "multi" ]; then
    composer_file="${composer_file} -f ${comp_base}.multinet.yml"
fi
if [ "$1" = "stop" ]; then
	echo Stoping running  cluster..
	docker-compose -f ${composer_file} down
	exit 0;
fi

if [ "$TARGET_CNT" -eq 0 ]; then
    echo Enter number of cache servers:
    read targetcount
    if ! [[ "$targetcount" =~ ^[0-9]+$ ]] ; then
      echo "Error: '$targetcount' is not a number"; exit 1
    fi
    TARGET_CNT=$targetcount
fi

if [ "$PROXY_CNT" -eq 0 ]; then
    echo Enter number of proxy servers:
    read proxycount
    if ! [[ "$proxycount" =~ ^[0-9]+$ ]] ; then
      echo "Error: '$proxycount' must be at least 1"; exit 1
    elif [ $proxycount -lt 1 ] ; then
      echo "Error: $proxycount is less than 1"; exit 1
    fi
    PROXY_CNT=$proxycount
fi

START=0
END=$TARGET_CNT

testfspathcnt=0
fspath="\"\":\"\""
if [ "$FS_LIST" = "" ] && [ "$TESTFS_CNT" -eq 0 ]; then
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
else
    if [ "$TESTFS_CNT" -eq 0 ]; then
       fspath=""
       IFS=',' read -r -a array <<< "$FS_LIST"
       for element in "${array[@]}"
       do
          fspath="$fspath,\"$element\" : \"\" "
       done
       fspath=${fspath#","}
   else
       testfspathcnt=$TESTFS_CNT
   fi
fi

FSPATHS=$fspath
echo $FSPATHS
TESTFSPATHCOUNT=$testfspathcnt

if [ $USE_AWS -eq 1 ]
then
  if [ -z "$aws_env" ]; then
    echo -a is a required parameter.Provide the path for aws.env file
    usage
  fi

  CLDPROVIDER="aws"
  cp $aws_env ${LOCAL_AWS}

  sed -i 's/\[default\]//g' ${LOCAL_AWS}
  sed -i 's/ = /=/g' ${LOCAL_AWS}
  sed -i 's/aws_access_key_id/AWS_ACCESS_KEY_ID/g' ${LOCAL_AWS}
  sed -i 's/aws_secret_access_key/AWS_SECRET_ACCESS_KEY/g' ${LOCAL_AWS}
  sed -i 's/region/AWS_DEFAULT_REGION/g' ${LOCAL_AWS}
else
  CLDPROVIDER="gcp"
fi

CONFFILE="dfc.json"
c=0
source $DIR/../../dfc/setup/config.sh

export TARGET_CNT=$TARGET_CNT

echo Stoping running  cluster..
docker-compose -f ${composer_file} down
echo Building Image..
docker-compose -f ${composer_file} build
echo Starting Primary Proxy
DFCPRIMARYPROXY=TRUE docker-compose -f ${composer_file} up -d dfcproxy
echo Starting cluster ..
docker-compose -f ${composer_file} up -d --scale dfctarget=$TARGET_CNT --scale dfcproxy=$PROXY_CNT --no-recreate
sleep 3

rm $CONFFILE $CONFFILE_STATSD $CONFFILE_COLLECTD
docker ps

echo done
