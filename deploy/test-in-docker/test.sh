#!/bin/bash

S_PATH=$(cd "$(dirname "$0")"; pwd -P)
AISTORE_PATH=$(cd "$S_PATH/../../.."; pwd -P)
CONTAINER_NAME=docker_test
CLD_PROVIDER=0
RUN_FLAGS=""
GIT_COMMIT=""

for i in "$@"; do
case ${i} in
    --name=*)
        CONTAINER_NAME="${i#*=}"
        # container name is supposed to be the same as the name of git branch
        GIT_COMMIT="[$CONTAINER_NAME] "$(git log -n 1 ${CONTAINER_NAME} --format="%s #%h")
        shift # past argument=value
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
        sed -i 's/region/AWS_REGION/g' ${TMP_FILE}

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

if [ "$(docker ps -q -f name=${CONTAINER_NAME}$)" ]; then
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
docker build -t test-docker -f ${S_PATH}/Dockerfile ${AISTORE_PATH} \
    --build-arg cld_provider=${CLD_PROVIDER}
cleanup
set +e # now we can allow fails

docker run -it ${RUN_FLAGS} \
    -v ${CONTAINER_NAME}:/tmp \
    --ulimit nofile=100000:100000 \
    --name=${CONTAINER_NAME} \
    --privileged \
    test-docker # image

echo -e "\n${GIT_COMMIT}"
if [ $? -ne 0 ]; then
    echo -e "\nSome tests did not pass :("
else
    echo -e "\nALL TESTS PASSED!"
fi

# Removing container and volume
docker rm -f ${CONTAINER_NAME} > /dev/null 2>&1
docker volume rm ${CONTAINER_NAME} > /dev/null 2>&1
