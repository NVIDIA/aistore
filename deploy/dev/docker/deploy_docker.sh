#!/bin/bash
name=`basename "$0"`
usage() {
    echo "Usage: $name [-a=AWS_DIR] [-c=NUM] [-d=NUM] [-f=LIST] [-g] [-h] [-l] [-m] [-p=NUM] [-s] [-t=NUM] [-qs=AWS_DIR (Optional)]"
    echo "  -a=AWS_DIR or --aws=AWS_DIR             : to use AWS, where AWS_DIR is the location of AWS configuration and credential files"
    echo "  -c=NUM or --cluster=NUM                 : where NUM is the number of clusters"
    echo "  -d=NUM or --directories=NUM             : where NUM is the number of local cache directories"
    echo "  -f=LIST or --filesystems=LIST           : where LIST is a comma separated list of filesystems"
    echo "  -g or --gcp                             : to use GCP"
    echo "  -h or --help                            : show usage"
    echo "  -l or --last                            : redeploy using the arguments from the last ais docker deployment"
    echo "  -m or --multi                           : use multiple networks"
    echo "  -p=NUM or --proxy=NUM                   : where NUM is the number of proxies"
    echo "  -s or --single                          : use a single network"
    echo "  -t=NUM or --target=NUM                  : where NUM is the number of targets"
    echo "  -qs=AWS_DIR or --quickstart=AWS_DIR     : deploys a quickstart version of AIS with one proxy, one targe and one local file system"
    echo "  -grafana                                : starts Graphite and Grafana containers"
    echo "  -nodiskio=BOOL                          : run Dry-Run mode with disk IO is disabled (default = false)"
    echo "  -nonetio=BOOL                           : run Dry-Run mode with network IO is disabled (default = false)"
    echo "  -dryobjsize=SIZE                        : size of an object when a source is a 'fake' one."
    echo "                                            'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB. Default value is '8m'"
    echo "Note:"
    echo "   -if the -f or --filesystems flag is used, the -d or --directories flag is disabled and vice-versa"
    echo "   -if the -a or --aws flag is used, the -g or --gcp flag is disabled and vice-versa"
    echo "   -if both -f and -d or -a and -g are provided, the flag that is provided last will take precedence"
    echo "   -if the -s or --single and -m  or --multi flag are used, then multiple networks will take precedence"
    echo
    exit 1;
}

is_number() {
    if ! [[ "$1" =~ ^[0-9]+$ ]] ; then
      echo "Error: '$1' is not a number"; exit 1
    fi
}

is_size() {
    if [ -z "$1" ]; then
      DRYOBJSIZE="8m"
    elif ! [[ "$1" =~ ^[0-9]+[g|G|m|M|k|K]$ ]] ; then
      echo "Error: '$1' is not a valid size"; exit 1
    fi
}

save_setup() {
    echo "" > $setup_file
    echo "Saving setup"
    echo "CLUSTER_CNT=$CLUSTER_CNT" >> $setup_file
    echo "PROXY_CNT=$PROXY_CNT" >> $setup_file
    echo "TARGET_CNT=$TARGET_CNT" >> $setup_file
    echo "network=$network" >> $setup_file

    echo "CLDPROVIDER=$CLDPROVIDER" >> $setup_file
    echo "aws_env=$aws_env" >> $setup_file
    echo "USE_AWS=$USE_AWS" >> $setup_file

    echo "DRYRUN"=$DRYRUN >> $setup_file
    echo "NODISKIO"=$NODISKIO >> $setup_file
    echo "NONETIO"=$NONETIO >> $setup_file
    echo "DRYOBJSIZE"=$DRYOBJSIZE >> $setup_file

    echo "FS_LIST=$FS_LIST" >> $setup_file
    echo "TESTFSPATHCOUNT=$TESTFSPATHCOUNT" >> $setup_file
    echo "FSPATHS=$FSPATHS" >> $setup_file

    echo "PRIMARYHOSTIP=$PRIMARYHOSTIP" >> $setup_file
    echo "NEXTTIERHOSTIP=$NEXTTIERHOSTIP" >> $setup_file

    echo "PORT=$PORT" >> $setup_file
    echo "PORT_INTRA_CONTROL=$PORT_INTRA_CONTROL" >> $setup_file
    echo "PORT_INTRA_DATA=$PORT_INTRA_DATA" >> $setup_file
    echo "GRAFANA=$GRAFANA" >> $setup_file
    echo "Finished saving setup"
}

get_setup() {
    if [ -f $"$setup_file" ]; then
        source $setup_file
    else
        echo "No setup configuration found for your last docker deployment. Exiting..."
        exit 1
    fi
}

deploy_mode() {
    if $NODISKIO; then
        echo "Deployed in no disk IO mode with ${DRYOBJSIZE} fake object size."
    elif $NONETIO; then
        echo "Deployed in no network IO mode with ${DRYOBJSIZE} fake object size."
    else
        echo "Deployed in normal mode."
    fi
}

deploy_quickstart() {
    QS_AWSDIR=${1:-'~/.aws/'}
    QS_AWSDIR="${QS_AWSDIR/#\~/$HOME}"
    if docker ps | grep ais-quickstart > /dev/null 2>&1; then
        echo "Terminating old instance of quickstart cluster ..."
        ./stop_docker.sh -qs
    fi
    # Need dummy files to make Dockerfile quiet
    echo "" > ais.json
    echo "" > collectd.conf
    echo "" > statsd.conf
    echo "Building Docker image ..."
    docker build -t -q ais-quickstart --build-arg GOBASE=/go . > /dev/null 2>&1
    if [ ! -d "$QS_AWSDIR" ]; then
        echo "AWS credentials not found (tests may not work!) ..."
        docker run -di ais-quickstart:latest
    else
        echo "AWS credentials found (${QS_AWSDIR}), continuing ..."
        docker run -div ${QS_AWSDIR}credentials:/root/.aws/credentials -v ${QS_AWSDIR}config:/root/.aws/config ais-quickstart:latest
    fi
    echo "SSH into container ..."
    container_id=`docker ps | grep ais-quickstart | awk '{ print $1 }'`
    docker exec -it $container_id /bin/bash -c "echo 'Hello from AIS!'; /bin/bash;"
    rm ais.json collectd.conf statsd.conf
    exit 1
}


if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'Error: docker-compose is not installed.' >&2
  exit 1
fi

mkdir -p /tmp/docker_ais
CLOUD=0
CLUSTER_CNT=0
PROXY_CNT=0
TARGET_CNT=0
FS_LIST=""
TESTFSPATHCOUNT=0
network=""
LOCAL_AWS="/tmp/docker_ais/aws.env"
setup_file="/tmp/docker_ais/deploy.env"

aws_env="";
os="ubuntu"

GRAFANA=false

# Indicate which dry-run mode the cluster is running on
DRYRUN=0
NODISKIO=false
NONETIO=false
DRYOBJSIZE="8m"

for i in "$@"
do
case $i in
    -a=*|--aws=*)
        aws_env="${i#*=}"
        shift # past argument=value
        CLOUD=1
        ;;

    -g|--gcp)
        CLOUD=2
        shift # past argument
        ;;

    -nocloud)
        CLOUD=3
        shift # past argument
        ;;

    -c=*|--cluster=*)
        CLUSTER_CNT="${i#*=}"
        is_number $CLUSTER_CNT
        network="multi"
        shift # past argument=value
        ;;

    -d=*|--directories=*)
        TESTFSPATHCOUNT="${i#*=}"
        is_number $TESTFSPATHCOUNT
        FS_LIST=""
        shift # past argument=value
        ;;

    -f=*|--filesystems=*)
        FS_LIST="${i#*=}"
        TESTFSPATHCOUNT=0
        shift # past argument=value
        ;;

    -h|--help)
        usage
        shift # past argument
        ;;

    -l|--last)
        get_setup
        break
        shift # past argument
        ;;

    -m|--multi)
        network="multi"
        shift # past argument
        ;;

    -p=*|--proxy=*)
        PROXY_CNT="${i#*=}"
        is_number $PROXY_CNT
        shift # past argument=value
        ;;

    -qs=*|--quickstart=*|-qs|--quickstart)
        deploy_quickstart "${i#*=}"
        break
        shift
        ;;

    -s|--single)
        if [ "$network" != "multi" ]; then
            network="single"
        fi
        shift # past argument=value
        ;;

    -t=*|--target=*)
        TARGET_CNT="${i#*=}"
        is_number $TARGET_CNT
        shift # past argument=value
        ;;

    -grafana)
        GRAFANA=true
        shift # past argument
        ;;

    -nodiskio=*|--nodiskio=*)
        NODISKIO="${i#*=}"
        if $NODISKIO; then
            DRYRUN=1
        fi
        shift # past argument=value
        ;;

    -nonetio=*|--nonetio=*)
        NONETIO="${i#*=}"
        if $NONETIO; then
            DRYRUN=2
        fi
        shift # past argument=value
        ;;

    -dryobjsize=*|--dryobjsize=*)
        DRYOBJSIZE="${i#*=}"
        is_size $DRYOBJSIZE
        shift # past argument=value
        ;;

    *)
        usage
        ;;
esac
done



if [ $DRYRUN -ne 0 ]; then
    echo "Configure Dry Run object size (default is '8m' - 8 megabytes):"
    echo "Note: 'g' or 'G' - GiB, 'm' or 'M' - MiB, 'k' or 'K' - KiB"
    echo "No input will result in using the default size"
    read DRYOBJSIZE
    is_size $DRYOBJSIZE
fi

if [ $CLOUD -eq 0 ]; then
    echo Select
    echo  1: Use AWS
    echo  2: Use GCP
    echo  3: No cloud provider
    echo "Enter your provider choice (1, 2 or 3):"
    read CLOUD
    is_number $CLOUD
    if [ $CLOUD -ne 1 ] && [ $CLOUD -ne 2 ] && [ $CLOUD -ne 3 ]; then
        echo "Not a valid entry. Exiting..."
        exit 1
    fi

    if [ $CLOUD -eq 1 ]; then
        echo "Enter the location of your AWS configuration and credentials files:"
        echo "Note: No input will result in using the default aws dir (~/.aws/)"
        read aws_env

        if [ -z "$aws_env" ]; then
            aws_env="~/.aws/"
        fi
    fi

fi

if [ $CLOUD -eq 1 ]; then
    if [ -z "$aws_env" ]; then
        echo -a is a required parameter.Provide the path for aws.env file
        usage
    fi
    CLDPROVIDER="aws"
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
elif [ $CLOUD -eq 2 ]; then
    CLDPROVIDER="gcp"
    touch $LOCAL_AWS
else
    CLDPROVIDER=""
    touch $LOCAL_AWS
fi

if [ "$CLUSTER_CNT" -eq 0 ]; then
    echo Enter number of AIStore clusters:
    read CLUSTER_CNT
    is_number $CLUSTER_CNT
    if [ "$CLUSTER_CNT" -gt 1 ]; then
        network="multi"
    fi
fi

if [[ -z "${network// }" ]]; then
	echo Enter s for single network configuration or m for multi-network configuration..
    read networkConfig
	if [ "$networkConfig" = "s" ]; then
        network="single"
    elif [ $networkConfig = 'm' ] ; then
        network="multi"
    else
        echo Valid network configuration was not supplied.
        usage
    fi
fi

if [ "$TARGET_CNT" -eq 0 ]; then
    echo Enter number of target servers:
    read TARGET_CNT
    is_number $TARGET_CNT
fi

if [ "$PROXY_CNT" -eq 0 ]; then
    echo Enter number of proxy servers:
    read PROXY_CNT
    is_number $PROXY_CNT
    if [ $PROXY_CNT -lt 1 ] ; then
      echo "Error: $PROXY_CNT is less than 1"; exit 1
    fi
fi

FSPATHS="\"\":\"\""
if [ "$FS_LIST" = "" ] && [ "$TESTFSPATHCOUNT" -eq 0 ]; then
    echo Select
    echo  1: Local cache directories
    echo  2: Filesystems
    echo "Enter your cache choice (1 or 2):"
    read cachesource
    is_number $cachesource
    if [ $cachesource -eq 1 ]; then
       echo Enter number of local cache directories:
       read TESTFSPATHCOUNT
       is_number $TESTFSPATHCOUNT
    elif [ $cachesource -eq 2 ]; then
       echo Enter filesystem info in comma separated format ex: /tmp/ais1,/tmp/ais:
       read FS_LIST
    else
        echo "Not a valid entry. Exiting..."
        exit 1
    fi
fi

if [ "$FS_LIST" != "" ] && [ "$TESTFSPATHCOUNT" -eq 0 ]; then
    FSPATHS=""
    IFS=',' read -r -a array <<< "$FS_LIST"
    for element in "${array[@]}"
    do
        FSPATHS="$FSPATHS,\"$element\" : \"\" "
    done
    FSPATHS=${FSPATHS#","}
fi

composer_file="${GOPATH}/src/github.com/NVIDIA/aistore/deploy/dev/docker/docker-compose.singlenet.yml"
if [ "$network" = "multi" ]; then
    composer_file="${GOPATH}/src/github.com/NVIDIA/aistore/deploy/dev/docker/docker-compose.singlenet.yml -f ${GOPATH}/src/github.com/NVIDIA/aistore/deploy/dev/docker/docker-compose.multinet.yml"
fi

PWD=$(pwd)
DIR=$(dirname "${BASH_SOURCE[0]}")
DIR="${PWD}/${DIR}"
echo $DIR
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
if [ "${PWD##*/}" != "docker" ]; then
    cd $DIR
fi

SERVICENAME="ais"
LOGDIR="/tmp/ais/log"
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
TESTFSPATHROOT="/tmp/ais/"
c=0

START=0
END=$TARGET_CNT

PORT=8080
PORT_INTRA_CONTROL=9080
PORT_INTRA_DATA=10080

if [ "$GRAFANA" == true ]; then
    GRAPHITE_PORT=2003
    GRAPHITE_SERVER="graphite"
    docker network create docker_default
    docker-compose -f ${composer_file} up --build -d graphite
    docker-compose -f ${composer_file} up --build -d grafana
else
    GRAPHITE_PORT=2003
    GRAPHITE_SERVER="localhost"
fi

# Setting the IP addresses for the containers
echo "Network type: ${network}"
for ((i=0; i<${CLUSTER_CNT}; i++)); do
    PUB_NET="172.5$((0 + ($i * 3))).0"
    PUB_SUBNET="${PUB_NET}.0/24"
    INT_CONTROL_NET="172.5$((1 + ($i * 3))).0"
    INT_CONTROL_SUBNET="${INT_CONTROL_NET}.0/24"
    INT_DATA_NET="172.5$((2 + ($i * 3))).0"
    INT_DATA_SUBNET="${INT_DATA_NET}.0/24"

    if [ $i -eq 0 ]; then
        PRIMARYHOSTIP="${PUB_NET}.2"
    fi
    if [ $i -eq 1 ]; then
        NEXTTIERHOSTIP="${PUB_NET}.2"
    fi

    PROXYURL="http://${PUB_NET}.2:${PORT}"

    IPV4LIST=""
    IPV4LIST_INTRA_CONTROL=""
    IPV4LIST_INTRA_DATA=""

    for j in `seq 2 $((($TARGET_CNT + $PROXY_CNT + 1) * $CLUSTER_CNT))`; do
        IPV4LIST="${IPV4LIST}${PUB_NET}.$j,"
    done
    if [ "$IPV4LIST" != "" ]; then
        IPV4LIST=${IPV4LIST::-1} # remove last ","
    fi

    if [ "$network" = "multi" ]; then
        # IPV4LIST_INTRA
        for j in `seq 2 $((($TARGET_CNT + $PROXY_CNT + 1) * $CLUSTER_CNT))`; do
            IPV4LIST_INTRA_CONTROL="${IPV4LIST_INTRA_CONTROL}${INT_CONTROL_NET}.$j,"
        done
        IPV4LIST_INTRA_CONTROL=${IPV4LIST_INTRA_CONTROL::-1} # remove last ","

        #IPV4LIST_INTRA_DATA
        for j in `seq 2 $((($TARGET_CNT + $PROXY_CNT + 1) * $CLUSTER_CNT))`; do
            IPV4LIST_INTRA_DATA="${IPV4LIST_INTRA_DATA}${INT_DATA_NET}.$j,"
        done
        IPV4LIST_INTRA_DATA=${IPV4LIST_INTRA_DATA::-1} # remove last ","
    fi

    echo "Public network: ${PUB_SUBNET}"
    echo "Internal control network: ${INT_CONTROL_SUBNET}"
    echo "Internal data network: ${INT_DATA_SUBNET}"
    export PUB_SUBNET=$PUB_SUBNET
    export INT_CONTROL_SUBNET=$INT_CONTROL_SUBNET
    export INT_DATA_SUBNET=$INT_DATA_SUBNET
    export TARGET_CNT=$TARGET_CNT
    export CLDPROVIDER=$CLDPROVIDER
    export MIRROR_ENABLED=false
    export MIRROR_UTIL_THRESH=20
    export IOSTAT_TIME="2s"
    export NODISKIO=$NODISKIO
    export NONETIO=$NONETIO
    export DRYOBJSIZE=$DRYOBJSIZE

    CONFFILE="ais.json"
    source $DIR/../../../ais/setup/config.sh

    echo Stopping running clusters...
    docker-compose -p ais${i} -f ${composer_file} down

    echo Building Image..
    docker-compose -p ais${i} -f ${composer_file} build

    echo Starting Primary Proxy
    export HOST_CONTAINER_PATH=/tmp/ais/c${i}_proxy_1
    mkdir -p $HOST_CONTAINER_PATH
    AIS_PRIMARYPROXY=TRUE docker-compose -p ais${i} -f ${composer_file} up --build -d proxy
    sleep 5 # give primary proxy some room to breath

    echo Starting cluster ..
    for ((j=1; j<=${TARGET_CNT}; j++)); do
        export HOST_CONTAINER_PATH=/tmp/ais/c${i}_target_${j}
        mkdir -p $HOST_CONTAINER_PATH
        docker-compose -p ais${i} -f ${composer_file} up --build -d --scale target=${j}  --scale grafana=0 --scale graphite=0 --no-recreate
    done
    for ((j=2; j<=${PROXY_CNT}; j++)); do
        export HOST_CONTAINER_PATH=/tmp/ais/c${i}_proxy_${j}
        mkdir -p $HOST_CONTAINER_PATH
        docker-compose -p ais${i} -f ${composer_file} up --build -d --scale proxy=${j} --scale target=$TARGET_CNT --scale grafana=0 --scale graphite=0 --no-recreate
    done
done

sleep 5
# Records all environment variables into $setup_file
save_setup

if [ "$CLUSTER_CNT" -gt 1 ] && [ "$network" = "multi" ]; then
    echo Connecting clusters together...
    for container_name in $(docker ps --format "{{.Names}}"); do
        container_id=$(docker ps -aqf "name=${container_name}")
        for ((i=0; i<${CLUSTER_CNT}; i++)); do
            if [[ $container_name != ais${i}_* ]]; then
                echo Connecting $container_name to $ais${i}_public
                docker network connect ais${i}_public $container_id
                if [[ $container_name == *"_target_"* ]]; then
                    echo Connecting $container_name to $ais${i}_internal_data
                    docker network connect ais${i}_internal_data $container_id
                fi
            fi
        done
    done
fi

if [ "$GRAFANA" == true ]; then
    # Set up Graphite datasource
    curl -d '{"name":"Graphite","type":"graphite","url":"http://graphite:80","access":"proxy","basicAuth":false,"isDefault":true}' -H "Content-Type: application/json" -X POST http://admin:admin@localhost:3000/api/datasources > /dev/null 2>&1
fi

# Consider moving these to a folder instead of deleting - for future reference
rm $CONFFILE $CONFFILE_STATSD $CONFFILE_COLLECTD
docker ps

deploy_mode
echo done
