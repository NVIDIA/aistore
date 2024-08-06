#!/bin/bash

set -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
name=$(basename "$0")

usage() {
    echo "Usage: $name [-c=NUM] [-d=NUM] [-f=LIST] [-g] [-h] [-l] [-m] [-p=NUM] [-s] [-t=NUM] [-qs=AWS_DIR (Optional)]"
    echo "  -c=NUM or --cluster=NUM                 : where NUM is the number of clusters"
    echo "  -d=NUM or --directories=NUM             : where NUM is the number of local cache directories"
    echo "  -f=LIST or --filesystems=LIST           : where LIST is a comma separated list of filesystems"
    echo "  -h or --help                            : show usage"
    echo "  -l or --last                            : redeploy using the arguments from the last ais docker deployment"
    echo "  -m or --multi                           : use multiple networks"
    echo "  -p=NUM or --proxy=NUM                   : where NUM is the number of proxies"
    echo "  -s or --single                          : use a single network"
    echo "  -t=NUM or --target=NUM                  : where NUM is the number of targets"
    echo "  -qs=AWS_DIR or --quickstart=AWS_DIR     : deploys a quickstart version of AIS with one proxy, one targe and one local file system"
    echo "  -nocloud                                : to deploy AIS without any 3rd party backend provider"
    echo "  -grafana                                : starts Graphite and Grafana containers"
    echo "  -nodiskio=BOOL                          : run Dry-Run mode with disk IO is disabled (default = false)"
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

save_env() {
    echo "Public network: ${PUB_SUBNET}"
    echo "Internal control network: ${INT_CONTROL_SUBNET}"
    echo "Internal data network: ${INT_DATA_SUBNET}"
    export PUB_SUBNET=${PUB_SUBNET}
    export INT_CONTROL_SUBNET=${INT_CONTROL_SUBNET}
    export INT_DATA_SUBNET=${INT_DATA_SUBNET}

    echo "" > ${TMP_ENV}
    echo "QUICK=${QUICK}" >> ${TMP_ENV}
    echo "TARGET_CNT=${TARGET_CNT:-1000}" >> ${TMP_ENV}
    echo "AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS-}" >> ${TMP_ENV}
    echo "TEST_FSPATH_COUNT=${TEST_FSPATH_COUNT}" >> ${TMP_ENV}
    echo "AIS_FS_PATHS=${AIS_FS_PATHS}" >> ${TMP_ENV}

    echo "NODISKIO=${NODISKIO-false}" >> ${TMP_ENV}
    echo "DRYOBJSIZE=${DRYOBJSIZE-8m}" >> ${TMP_ENV}

    echo "GRAPHITE_PORT=${GRAPHITE_PORT}" >> ${TMP_ENV}
    echo "GRAPHITE_SERVER=${GRAPHITE_SERVER}" >> ${TMP_ENV}

    echo "PUB_SUBNET=${PUB_SUBNET}" >> ${TMP_ENV}
    echo "INT_CONTROL_SUBNET=${INT_CONTROL_SUBNET}" >> ${TMP_ENV}
    echo "INT_DATA_SUBNET=${INT_DATA_SUBNET}" >> ${TMP_ENV}
    echo "HOSTNAME_LIST=${HOSTNAME_LIST}" >> ${TMP_ENV}
    echo "HOSTNAME_LIST_INTRA_CONTROL=${HOSTNAME_LIST_INTRA_CONTROL}" >> ${TMP_ENV}
    echo "HOSTNAME_LIST_INTRA_DATA=${HOSTNAME_LIST_INTRA_DATA}" >> ${TMP_ENV}
}

save_setup() {
    echo "" > ${SETUP_FILE}
    echo "Saving setup"
    echo "CLUSTER_CNT=$CLUSTER_CNT" >> ${SETUP_FILE}
    echo "PROXY_CNT=$PROXY_CNT" >> ${SETUP_FILE}
    echo "TARGET_CNT=$TARGET_CNT" >> ${SETUP_FILE}
    echo "NETWORK=${NETWORK}" >> ${SETUP_FILE}

    echo "AIS_BACKEND_PROVIDERS=$AIS_BACKEND_PROVIDERS" >> ${SETUP_FILE}

    echo "DRYRUN"=$DRYRUN >> ${SETUP_FILE}
    echo "NODISKIO"=$NODISKIO >> ${SETUP_FILE}
    echo "DRYOBJSIZE"=$DRYOBJSIZE >> ${SETUP_FILE}

    echo "FS_LIST=$FS_LIST" >> ${SETUP_FILE}
    echo "TEST_FSPATH_COUNT=${TEST_FSPATH_COUNT}" >> ${SETUP_FILE}
    echo "AIS_FS_PATHS=$AIS_FS_PATHS" >> ${SETUP_FILE}

    echo "PRIMARY_HOST_IP=${PRIMARY_HOST_IP}" >> ${SETUP_FILE}
    echo "NEXT_TIER_HOST_IP=${NEXT_TIER_HOST_IP}" >> ${SETUP_FILE}

    echo "PORT=$PORT" >> ${SETUP_FILE}
    echo "PORT_INTRA_CONTROL=$PORT_INTRA_CONTROL" >> ${SETUP_FILE}
    echo "PORT_INTRA_DATA=$PORT_INTRA_DATA" >> ${SETUP_FILE}
    echo "Finished saving setup"
}

get_setup() {
    if [ -f $"${SETUP_FILE}" ]; then
        source ${SETUP_FILE}
    else
        echo "No setup configuration found for your last docker deployment. Exiting..."
        exit 1
    fi
}

deploy_mode() {
    if $NODISKIO; then
        echo "Deployed in no disk IO mode with ${DRYOBJSIZE} fake object size."
    else
        echo "Deployed in normal mode."
    fi
}

deploy_quickstart() {
    cp $DIR/../utils.sh utils.sh
    cp $DIR/../local/aisnode_config.sh aisnode_config.sh
    cp $DIR/../../conf/limits.conf limits.conf

    QS_AWSDIR=${1:-'~/.aws/'}
    QS_AWSDIR="${QS_AWSDIR/#\~/$HOME}"
    if docker ps | grep ais-quickstart > /dev/null 2>&1; then
        echo "Terminating old instance of quickstart cluster ..."
        ./stop_docker.sh -qs
    fi
    echo "Building Docker image ..."
    docker build -q -t ais-quickstart --build-arg GOBASE=/go --build-arg QUICK=quick .
    if [ ! -d "$QS_AWSDIR" ]; then
        echo "AWS credentials not found (tests may not work!) ..."
        docker run -di  --entrypoint=/bin/bash ais-quickstart:latest
    else
        echo "AWS credentials found (${QS_AWSDIR}), continuing ..."
        docker run -di --entrypoint=/bin/bash -v ${QS_AWSDIR}credentials:/root/.aws/credentials -v ${QS_AWSDIR}config:/root/.aws/config ais-quickstart:latest
    fi

    echo "SSH into container ..."
    container_id=$(docker ps | grep ais-quickstart | awk '{ print $1 }')
    docker exec -it $container_id /bin/bash -c "echo 'Hello from AIS!'; /bin/bash;"

    rm -rf utils.sh
    rm -rf aisnode_config.sh
    rm -rf limits.conf
}


if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'Error: docker-compose is not installed.' >&2
  exit 1
fi

AIS_BACKEND_PROVIDERS=""
CLUSTER_CNT=0
PROXY_CNT=0
TARGET_CNT=0
FS_LIST=""
TEST_FSPATH_COUNT=1
NETWORK=""
AWS_ENV=""

mkdir -p /tmp/docker_ais
LOCAL_AWS="/tmp/docker_ais/aws.env"
SETUP_FILE="/tmp/docker_ais/deploy.env"
TMP_ENV="/tmp/docker_ais/tmp.env"
touch ${TMP_ENV}

GRAFANA=false

# Indicate which dry-run mode the cluster is running on
DRYRUN=0
NODISKIO=false
DRYOBJSIZE="8m"

source ../utils.sh

for i in "$@"
do
case $i in
    -c=*|--cluster=*)
        CLUSTER_CNT="${i#*=}"
        is_number $CLUSTER_CNT
        NETWORK="multi"
        shift # past argument=value
        ;;

    -d=*|--directories=*)
        TEST_FSPATH_COUNT="${i#*=}"
        is_number $TEST_FSPATH_COUNT
        FS_LIST=""
        shift # past argument=value
        ;;

    -f=*|--filesystems=*)
        FS_LIST="${i#*=}"
        TEST_FSPATH_COUNT=0
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
        NETWORK="multi"
        shift # past argument
        ;;

    -p=*|--proxy=*)
        PROXY_CNT="${i#*=}"
        is_number $PROXY_CNT
        shift # past argument=value
        ;;

    -qs=*|--quickstart=*|-qs|--quickstart)
        deploy_quickstart "${i#*=}"
        exit 1
        ;;

    -s|--single)
        if [ "${NETWORK}" != "multi" ]; then
            NETWORK="single"
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
        export NODISKIO="${i#*=}"
        if $NODISKIO; then
            DRYRUN=1
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

set_env_backends

touch $LOCAL_AWS
echo "Configured backend providers: '${AIS_BACKEND_PROVIDERS}'"
if [[ "${AIS_BACKEND_PROVIDERS}" == *aws* ]]; then
    echo "Enter the location of your AWS configuration and credentials files:"
    echo "Note: No input will result in using the default aws dir (~/.aws/)"
    read AWS_ENV

    if [[ -z ${aws_env} ]]; then
        AWS_ENV="${HOME}/.aws/"
    fi

    AWS_ENV="${AWS_ENV/#\~/$HOME}"
    temp_file="${AWS_ENV}/credentials"
    has_reg=0
    if [[ -f ${temp_file} ]]; then
        cp ${temp_file} ${LOCAL_AWS}
        has_reg=$(cat ${temp_file} | grep -c "region")
    else
        echo "No AWS credentials file found in specified directory. Exiting..."
        exit 1
    fi

    if [[ $has_reg -eq 0 ]]; then
        temp_file="${AWS_ENV}/config"
        if [[ -f ${temp_file} ]] && [[ $(cat ${temp_file} | grep -c "region") -gt 0 ]]; then
            grep region ${temp_file} >> ${LOCAL_AWS}
        else
            echo "No region config field found in aws directory. Exiting..."
            exit 1
        fi
    fi

    sed -i 's/\[default\]//g' ${LOCAL_AWS}
    sed -i 's/ = /=/g' ${LOCAL_AWS}
    sed -i 's/aws_access_key_id/AWS_ACCESS_KEY_ID/g' ${LOCAL_AWS}
    sed -i 's/aws_secret_access_key/AWS_SECRET_ACCESS_KEY/g' ${LOCAL_AWS}
    sed -i 's/region/AWS_REGION/g' ${LOCAL_AWS}
fi

if [ "$CLUSTER_CNT" -eq 0 ]; then
    echo Enter number of AIStore clusters:
    read CLUSTER_CNT
    is_number $CLUSTER_CNT
    if [ "$CLUSTER_CNT" -gt 1 ]; then
        NETWORK="multi"
    fi
fi

if [[ -z "${NETWORK// }" ]]; then
    echo "Enter 's' for single network configuration or 'm' for multi-network configuration:"
    read network_config
    if [ "$network_config" = "s" ]; then
        NETWORK="single"
    elif [ $network_config = 'm' ] ; then
        NETWORK="multi"
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

AIS_FS_PATHS=""
if [ "$FS_LIST" = "" ] && [ "$TEST_FSPATH_COUNT" -eq 0 ]; then
    echo Select
    echo  1: Local cache directories
    echo  2: Filesystems
    echo "Enter your cache choice (1 or 2):"
    read cachesource
    is_number $cachesource
    if [ $cachesource -eq 1 ]; then
       echo Enter number of local cache directories:
       read TEST_FSPATH_COUNT
       is_number $TEST_FSPATH_COUNT
    elif [ $cachesource -eq 2 ]; then
       echo Enter filesystem info in comma separated format ex: /tmp/ais1,/tmp/ais:
       read FS_LIST
    else
        echo "Not a valid entry. Exiting..."
        exit 1
    fi
fi

if [ "$FS_LIST" != "" ] && [ "$TEST_FSPATH_COUNT" -eq 0 ]; then
    AIS_FS_PATHS=""
    IFS=',' read -r -a array <<< "$FS_LIST"
    for element in "${array[@]}"
    do
        AIS_FS_PATHS="$AIS_FS_PATHS,\"$element\" : {} "
    done
    AIS_FS_PATHS=${AIS_FS_PATHS#","}
fi

composer_file="${DIR}/docker-compose.singlenet.yml"
if [ "${NETWORK}" = "multi" ]; then
    composer_file="${DIR}/docker-compose.singlenet.yml -f ${DIR}/docker-compose.multinet.yml"
fi

cp $DIR/../utils.sh utils.sh
cp $DIR/../local/aisnode_config.sh aisnode_config.sh
cp $DIR/../../conf/limits.conf limits.conf

docker network create docker_default || true
if [ "$GRAFANA" == true ]; then
    GRAPHITE_PORT=2003
    GRAPHITE_SERVER="graphite"
    docker-compose -f ${composer_file} up --build -d graphite
    docker-compose -f ${composer_file} up --build -d grafana
else
    GRAPHITE_PORT=2003
    GRAPHITE_SERVER="localhost"
fi

PORT_INTRA_CONTROL=9080
PORT_INTRA_DATA=10080
export PORT=51080
export AIS_NO_DISK_IO=${NODISKIO}
export AIS_BACKEND_PROVIDERS=${AIS_BACKEND_PROVIDERS}
# Setting the IP addresses for the containers
echo "Network type: ${NETWORK}"
for ((i=0; i<${CLUSTER_CNT}; i++)); do
    PUB_NET="172.5$((0 + (i * 3))).0"
    PUB_SUBNET="${PUB_NET}.0/24"
    INT_CONTROL_NET="172.5$((1 + (i * 3))).0"
    INT_CONTROL_SUBNET="${INT_CONTROL_NET}.0/24"
    INT_DATA_NET="172.5$((2 + (i * 3))).0"
    INT_DATA_SUBNET="${INT_DATA_NET}.0/24"

    if [ $i -eq 0 ]; then
        PRIMARY_HOST_IP="${PUB_NET}.2"
    fi
    if [ $i -eq 1 ]; then
        NEXT_TIER_HOST_IP="${PUB_NET}.2"
    fi

    HOSTNAME_LIST=""
    HOSTNAME_LIST_INTRA_CONTROL=""
    HOSTNAME_LIST_INTRA_DATA=""

    mkdir -p /tmp/ais/${i}
    # UID is used to ensure that volumes' folders have the same permissions as
    # the user who starts the script. Otherwise they would have `root` permission.
    export UID # (see docker compose files)
    export CLUSTER=${i}

    for j in $(seq 2 $(((TARGET_CNT + PROXY_CNT + 1) * CLUSTER_CNT))); do
        HOSTNAME_LIST="${HOSTNAME_LIST}${PUB_NET}.$j,"
    done
    if [ "$HOSTNAME_LIST" != "" ]; then
        HOSTNAME_LIST=${HOSTNAME_LIST/%?/} # remove last ","
    fi

    if [ "${NETWORK}" = "multi" ]; then
        # HOSTNAME_LIST_INTRA
        for j in $(seq 2 $(((TARGET_CNT + PROXY_CNT + 1) * CLUSTER_CNT))); do
            HOSTNAME_LIST_INTRA_CONTROL="${HOSTNAME_LIST_INTRA_CONTROL}${INT_CONTROL_NET}.$j,"
        done
        HOSTNAME_LIST_INTRA_CONTROL=${HOSTNAME_LIST_INTRA_CONTROL/%?/} # remove last ","

        # HOSTNAME_LIST_INTRA_DATA
        for j in $(seq 2 $(((TARGET_CNT + PROXY_CNT + 1) * CLUSTER_CNT))); do
            HOSTNAME_LIST_INTRA_DATA="${HOSTNAME_LIST_INTRA_DATA}${INT_DATA_NET}.$j,"
        done
        HOSTNAME_LIST_INTRA_DATA=${HOSTNAME_LIST_INTRA_DATA/%?/} # remove last ","
    fi

    save_env

    echo Stopping running clusters...
    docker-compose -p ais${i} -f ${composer_file} down

    echo Building Image..
    docker-compose -p ais${i} -f ${composer_file} build

    echo Starting Primary Proxy
    docker-compose -p ais${i} -f ${composer_file} up -d proxy
    sleep 2 # give primary proxy some room to breathe
    echo Starting cluster ..
    PRIMARY_IP=$(docker inspect -f "{{ .NetworkSettings.Networks.ais${i}_public.IPAddress }}" ais${i}_proxy_1)
    PRIMARY_IP=${PRIMARY_IP} docker-compose -p ais${i} -f ${composer_file} up -d --scale proxy=${PROXY_CNT} --scale target=$TARGET_CNT --scale grafana=0 --scale graphite=0 --no-recreate
done

sleep 5
# Records all environment variables into ${SETUP_FILE}
save_setup

if [ "$CLUSTER_CNT" -gt 1 ] && [ "${NETWORK}" = "multi" ]; then
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
rm utils.sh
rm aisnode_config.sh
rm limits.conf
docker ps

# Install the CLI
cd $DIR/../../../ && make cli

deploy_mode

for ((i=0; i<${CLUSTER_CNT}; i++)); do
    PRIMARY_IP=$(docker inspect -f "{{ .NetworkSettings.Networks.ais${i}_public.IPAddress }}" ais${i}_proxy_1)
    echo "Cluster${i} deployed! use 'AIS_ENDPOINT=\"http://${PRIMARY_IP}:${PORT}\" ais show cluster' to view cluster info"
done

echo done
