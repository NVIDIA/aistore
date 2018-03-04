#!/bin/bash

usage() {
    echo "Usage: $0 [-e dev|prod] -a <aws.env> [-s] [-o centos|ubuntu]"
    echo "   -e : dev|prod - deployment environment (default is dev[elopment])"
    echo "   -a : aws.env - AWS credentials"
    echo "   -o : centos|ubuntu - docker base image OS (default ubuntu)"
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
    e)
        environment=${OPTARG}
        ;;
    a)
        aws_env=${OPTARG}
        ;;

    o)
        os=${OPTARG}
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
	rm Dockerfile
	sudo docker ps
	echo Done
	exit 0
fi
if [ -z "$aws_env" ]; then
	echo -a is a required parameter.Provide the path for aws.env file
	usage
fi
# copy .deb or .rpm docker file as Dockerfile
if [ $os == "ubuntu" ]; then
	echo "Using debian packaging for the docker container"
	cp Dockerfile.deb Dockerfile
else
	echo "Usind rpm packaging for the docker container"
	cp Dockerfile.rpm Dockerfile
fi

PROXYURL="http://dfcproxy:8080"
PORT=8080
SERVICENAME="dfc"
LOGDIR="/tmp/dfc/log"
LOGLEVEL="3"
###################################
#
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TESTFSPATHROOT="/tmp/dfc/"

AWSENVFILE="aws.env"

echo Enter number of cache targets:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$servcount' is not a number"; exit 1
fi
START=0
END=$servcount

echo "Number of local cache directories (enter 0 to use preconfigured filesystems):"
read testfspathcnt
if ! [[ "$testfspathcnt" =~ ^[0-9]+$ ]] ; then
	echo "Error: '$testfspathcnt' is not a number"; exit 1
fi
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
elif [ $cldprovider -eq 2 ]
then
	CLDPROVIDER="gcp"
else
	echo "Error: '$cldprovider' is not a valid input, can be either 1 or 2"; exit 1
fi

CONFFILE="dfc.json"
c=0
source $DIR/../dfc/setup/config.sh

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
mv dfc.json /tmp/dfc_backup/
mv Dockerfile /tmp/dfc_backup/
echo done
