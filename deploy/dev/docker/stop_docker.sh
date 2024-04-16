#!/bin/bash
NAME=`basename "$0"`
SETUP_FILE="/tmp/docker_ais/deploy.env"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

usage() {
    echo "============================================== Usage: =============================================="
    echo "$name removes the containers, networks and images used by services defined in a compose file"
    echo "$name -s or --single              --> to stop a single network docker configuration"
    echo "$name -m or --multi               --> to stop a multi network docker configuration"
    echo "$name -c=NUM or--clustered=NUM    --> to stop multiple cluster docker configuration with NUM clusters"
    echo "$name -qs or --quickstart         --> stop the quickstart docker AIS deployment"
    echo "$name -l or --last                --> Uses your last saved docker configuration defined in ${SETUP_FILE} to stop docker"
    echo "Note adding the --rmi flag to any of the commands above will also removes images."
    echo "Note providing multiple flags will result in the last flag taking precedence."
    echo
    exit 1;
}

is_number() {
    if ! [[ "$1" =~ ^[0-9]+$ ]] ; then
      echo "Error: '$1' is not a number"; exit 1
    fi
}

get_setup() {
    if [ -f $"${SETUP_FILE}" ]; then
        source ${SETUP_FILE}
    else
        echo "No setup configuration found for your last docker deployment. Exiting..."
        exit 1
    fi
}

determine_config() {
    if [ "${NETWORK}" == "single" ]; then
        composer_file="${DIR}/docker-compose.singlenet.yml"
    elif [ "${NETWORK}" == "multi" ]; then
        composer_file="${DIR}/docker-compose.singlenet.yml -f ${DIR}/docker-compose.multinet.yml"
    else
        echo "ERROR: No docker configuration selected."
        usage
    fi
}

stop_quickstart() {
    container_id=`docker ps | grep ais-quickstart | awk '{ print $1 }'`
    if docker ps | grep ais-quickstart > /dev/null 2>&1; then
        docker rm --force $container_id
        echo "AIStore quickstart terminated"
    fi
    exit 1
}

if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'Error: docker-compose is not installed.' >&2
  exit 1
fi

CLUSTER_CNT=1
NETWORK=""
REMOVE_IMAGES=false
for i in "$@"
do
case $i in
    -c=*|--clustered=*)
        CLUSTER_CNT="${i#*=}"
        is_number CLUSTER_CNT
        NETWORK="multi"
        shift # past argument=value
        ;;

    -l|--last)
        get_setup
        shift
        ;;

    -s|--single)
        NETWORK="single"
        shift # past argument=value
        ;;

    -m|--multi)
        NETWORK="multi"
        shift # past argument=value
        ;;

    -qs|--quickstart)
        stop_quickstart
        shift
        ;;

    --rmi)
        REMOVE_IMAGES=true
        shift # past argument=value
        valid_log_file_type $LOG_TYPE
        ;;

    -s|--single)
        network="single"
        shift # past argument=value
        ;;


    *)
        usage
        ;;
esac
done

determine_config

if [ "$CLUSTER_CNT" -gt 1 ]; then
    echo "Removing connections between clusters..."
    for container_name in $(docker ps --format "{{.Names}}"); do
        container_id=$(docker ps -aqf "name=${container_name}")
        for ((i=0; i<${CLUSTER_CNT}; i++)); do
            if [[ $container_name != ais${i}_* ]]; then
                docker network disconnect -f ais${i}_public $container_id
                if [[ $container_name == *"_target_"* ]]; then
                    docker network disconnect -f ais${i}_internal_data $container_id
                fi
            fi
        done
    done
fi

for ((i=0; i<${CLUSTER_CNT}; i++)); do
    export PUB_SUBNET="172.5$((0 + ($i * 3))).0.0/24"
    export INT_CONTROL_SUBNET="172.5$((1 + ($i * 3))).0.0/24"
    export INT_DATA_SUBNET="172.5$((2 + ($i * 3))).0.0/24"
    if [ "$REMOVE_IMAGES" == true ]; then
        docker-compose -p ais${i} -f $composer_file down -v --rmi all --remove-orphans
    else
        docker-compose -p ais${i} -f $composer_file down -v --remove-orphans
    fi
done

if [ "$remove_images" = TRUE ]; then
    docker-compose -f $composer_file down -v --rmi all --remove-orphans
else
    docker-compose -f $composer_file down -v --remove-orphans
fi

echo "Removing volumes..."
docker volume prune -f
echo "Removing volumes folders..."
for ((i=0; i<${CLUSTER_CNT}; i++)); do
    rm -rf /tmp/ais/${i}
done

# Remove CLI
rm -f ${GOPATH}/bin/ais