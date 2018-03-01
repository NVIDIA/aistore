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

#default is dev
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


if [ "${PWD##*/}" != "docker" ]; then
    # Assuming we are in the dfc source directory
    cd ../docker/
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


#Copy .deb or .rpm docker file as Dockerfile. This is done keep docker_compose agnostic of docker OS
if [ $os == "ubuntu" ]; then
   echo "Using debian packaging for the docker container"
   cp Dockerfile.deb Dockerfile
else
   echo "Usind rpm packaging for the docker container"
   cp Dockerfile.rpm Dockerfile
fi 

TMPDIR="/tmp/nvidia"
PROXYURL="http://dfcproxy:8080"
PASSTHRU=true

# local daemon ports start from $PORT+1
PORT=8080

SERVICENAME="dfc"
PROTO="tcp"
DIRPATH="/tmp/nvidia"
###################################
#
# NOTE:
# fspaths config is used if and only if test_fspaths.count == 0
# existence of each fspath is checked at runtime
#
###################################
TESTFSPATHROOT="/tmp/dfc/"
# Verbosity: 0 (minimal) to 4 (max)
LOGLEVEL="3"

# Use log logir /var/log/dfc in produciton 
LOGDIR="/tmp/dfc/log"
CLOUDBUCKETS="cloud"
LOCALBUCKETS="local"
LBCONF="localbuckets"
MAXCONCURRENTDOWNLOAD=64
MAXCONCURRENTUPLOAD=64
MAXPARTSIZE=4294967296
STATSTIME="10s"
HTTPTIMEOUT="60s"
DONTEVICTIME="30m"
AWSENVFILE="aws.env"
LOWWATERMARK=75
HIGHWATERMARK=90
NOXATTRS=false
H2C=false



echo Enter number of cache servers:
read servcount
if ! [[ "$servcount" =~ ^[0-9]+$ ]] ; then
  echo "Error: '$servcount' is not a number"; exit 1
fi
START=0
END=$servcount

echo Test-only: enter number of local cache directories for each target:
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
cat > $CONFFILE <<EOL
  {
    "logdir":			"${LOGDIR}",
    "loglevel": 			"${LOGLEVEL}",
    "cloudprovider":		"${CLDPROVIDER}",
		"cloud_buckets":		"${CLOUDBUCKETS}",
		"local_buckets":		"${LOCALBUCKETS}",
		"lb_conf":                	"${LBCONF}",
		"stats_time":			"${STATSTIME}",
		"http_timeout":			"${HTTPTIMEOUT}",
    "listen": {
      "proto": 		"${PROTO}",
      "port":			"${PORT}"
    },
    "proxy": {
      "url": 			"${PROXYURL}",
      "passthru": 		${PASSTHRU}
    },
    "s3": {
      "maxconcurrdownld":	${MAXCONCURRENTDOWNLOAD},
      "maxconcurrupld":	${MAXCONCURRENTUPLOAD},
      "maxpartsize":		${MAXPARTSIZE}
    },
		"lru_config": {
			"lowwm":		${LOWWATERMARK},
			"highwm":		${HIGHWATERMARK},
			"dont_evict_time":	"${DONTEVICTIME}"
		},
		"test_fspaths": {
			"root":			"${TESTFSPATHROOT}",
			"count":		$TESTFSPATHCOUNT,
			"instance":		0
		},
		"fspaths": {
			"/zpools/vol1/a/b/c":	"",
			"/zpools/vol2/m/n/p":	""
		},
		"no_xattrs":			${NOXATTRS},
        "h2c":                  ${H2C}
  }
EOL

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

