#!/bin/bash
name=`basename "$0"`
setup_file="/tmp/docker_dfc/deploy.env"

usage() {
    echo "============================================== Usage: =============================================="
    echo "$name removes the containers, networks and images used by services defined in a compose file"
    echo "$name -s or --single              --> to stop a single network docker configuration"
    echo "$name -m or --multi               --> to stop a multi network docker configuration"
    echo "$name -c=NUM or--clustered=NUM    --> to stop multiple cluster docker configuration with NUM clusters"
    echo "$name -l or --last                --> Uses your last saved docker configuration defined in $setup_file to stop docker"
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
    if [ -f $"$setup_file" ]; then
        source $setup_file
    else
        echo "No setup configuration found for your last docker deployment. Exiting..."
        exit 1
    fi
}

determine_config() {
    if [ "$network" == "single" ]; then
        composer_file="${GOPATH}/src/github.com/NVIDIA/dfcpub/docker/dev/docker-compose.singlenet.yml"
    elif [ "$network" == "multi" ]; then
        composer_file="${GOPATH}/src/github.com/NVIDIA/dfcpub/docker/dev/docker-compose.singlenet.yml -f ${GOPATH}/src/github.com/NVIDIA/dfcpub/docker/dev/docker-compose.multinet.yml"
    else
        echo "ERROR: No docker configuration selected."
        usage
    fi
}

if ! [ -x "$(command -v docker-compose)" ]; then
  echo 'Error: docker-compose is not installed.' >&2
  exit 1
fi

CLUSTER_CNT=1
network=""
remove_images=FALSE
for i in "$@"
do
case $i in
    -c=*|--clustered=*)
        CLUSTER_CNT="${i#*=}"
        is_number CLUSTER_CNT
        network="multi"
        shift # past argument=value
        ;;

    -s|--single)
        network="single"
        shift # past argument=value
        ;;

    -m|--multi)
        network="multi"
        shift # past argument=value
        ;;

    --rmi)
        remove_images=TRUE
        shift # past argument=value
        valid_log_file_type $LOG_TYPE
        ;;

    -l|--last)
        get_setup
        shift # past argument=value
        ;;

    *)
        usage
        ;;
esac
done

determine_config

if [ "$CLUSTER_CNT" -gt 1 ]; then
    echo Removing connections between clusters ...
    for container_name in $(docker ps --format "{{.Names}}"); do
        container_id=$(docker ps -aqf "name=${container_name}")
        for ((i=0; i<${CLUSTER_CNT}; i++)); do
            if [[ $container_name != dfc${i}_* ]] ;
            then
                docker network disconnect -f dfc${i}_public $container_id
                if [[ $container_name == *"_target_"* ]] ;
                then
                    docker network disconnect -f dfc${i}_internal_data $container_id
                fi
            fi
        done
    done
fi

for ((i=0; i<${CLUSTER_CNT}; i++)); do
    PUB_NET="172.5$((0 + ($i * 3))).0"
    PUB_SUBNET="${PUB_NET}.0/24"
    INT_CONTROL_NET="172.5$((1 + ($i * 3))).0"
    INT_CONTROL_SUBNET="${INT_CONTROL_NET}.0/24"
    INT_DATA_NET="172.5$((2 + ($i * 3))).0"
    INT_DATA_SUBNET="${INT_DATA_NET}.0/24"
    export PUB_SUBNET=$PUB_SUBNET
    export INT_CONTROL_SUBNET=$INT_CONTROL_SUBNET
    export INT_DATA_SUBNET=$INT_DATA_SUBNET
    if [ "$remove_images" = TRUE ]; then
        docker-compose -p dfc${i} -f $composer_file down -v --rmi all --remove-orphans
    else 
        docker-compose -p dfc${i} -f $composer_file down -v --remove-orphans
    fi
done