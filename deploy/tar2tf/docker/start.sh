#!/bin/bash

S_PATH=$(cd "$(dirname "$0")"; pwd -P)
AISTORE_PATH=$(cd "$S_PATH/../../.."; pwd -P)
CONTAINER_NAME=ais-tar2tf
TAG_NAME=ais-tar2tf
CLD_PROVIDER=0
RUN_FLAGS=""
MOUNT_FLAG=""
DOCKER_DATA_DIR="/data/"

if [[ -n $(netstat --help 2>/dev/null) ]]; then
  [[ -n $(netstat -tulpn | grep :::8888 >/dev/null) ]] && echo "Make sure that nothing is listening on port 8888"
  exit 1
fi

for i in "$@"; do
case ${i} in
    --name=*)
        CONTAINER_NAME="${i#*=}"
        shift # past argument=value
        ;;

    --v=*)
        MOUNT_FLAG="-v ${i#*=}:${DOCKER_DATA_DIR}"
        shift
        ;;

    --aws=*)
        ENV_DIR="${i#*=}"
        ENV_DIR="${ENV_DIR/#\~/$HOME}"
        if [[ ! -d ${ENV_DIR} ]]; then
            echo "${ENV_DIR} is not directory"
            exit 1
        fi

        TMP_FILE=${ENV_DIR}/.aws.env
        cat ${ENV_DIR}/credentials > ${TMP_FILE}
        cat ${ENV_DIR}/config >> ${TMP_FILE}

        sed -i 's/\[default\]//g' ${TMP_FILE}
        sed -i 's/ = /=/g' ${TMP_FILE}
        sed -i 's/aws_access_key_id/AWS_ACCESS_KEY_ID/g' ${TMP_FILE}
        sed -i 's/aws_secret_access_key/AWS_SECRET_ACCESS_KEY/g' ${TMP_FILE}
        sed -i 's/region/AWS_DEFAULT_REGION/g' ${TMP_FILE}

        RUN_FLAGS="${RUN_FLAGS} --env-file ${TMP_FILE}"
        CLD_PROVIDER=1

        shift # past argument=value
        ;;

    -g|--gcp)
        CLD_PROVIDER=2
        shift # past argument
        ;;

    *)
        echo "Invalid usage"
        exit 1
esac
done

if [[ -n $(docker ps -q -f name=${CONTAINER_NAME}) ]]; then
    echo "Container with ${CONTAINER_NAME} name already exists/running"
    exit 1
fi

function cleanup {
    rm -f ${AISTORE_PATH}/.dockerignore
}
trap cleanup EXIT INT TERM

set -e # don't allow errors in build and volume creation
echo ".git" > ${AISTORE_PATH}/.dockerignore
docker volume create ${CONTAINER_NAME} # mount filesystem for docker so AIS can see that
docker build -t $TAG_NAME -f ${S_PATH}/Dockerfile ${AISTORE_PATH} \
    --build-arg cld_provider=${CLD_PROVIDER}
cleanup
set +e # now we can allow fails

docker run -it ${RUN_FLAGS} \
    $MOUNT_FLAG \
    --ulimit nofile=100000:100000 \
    --name=${CONTAINER_NAME} \
    --privileged \
    -p 8888:8888 \
    $TAG_NAME


# Removing container and volume
docker rm -f ${CONTAINER_NAME} > /dev/null 2>&1
docker volume rm ${CONTAINER_NAME} > /dev/null 2>&1
