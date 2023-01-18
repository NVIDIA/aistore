#!/bin/bash

BUCKET=docker_local_bucket
AISTORE_PATH=$GOPATH/src/github.com/NVIDIA/aistore

if [ ${CLD_PROVIDER} == 1 ]; then
    BUCKET=${HOSTNAME}
    aws s3api create-bucket --bucket ${BUCKET} --region ${AWS_DEFAULT_REGION} --create-bucket-configuration LocationConstraint=${AWS_DEFAULT_REGION}
elif [ ${CLD_PROVIDER} == 2 ]; then
    BUCKET=smth # TODO:
fi

function cleanup {
    if [ ${CLD_PROVIDER} == 1 ]; then
        aws s3 rb s3://${BUCKET} --force
    elif [ ${CLD_PROVIDER} == 2 ]; then
        : # TODO: currently noop
    fi
}
trap cleanup EXIT

pushd $AISTORE_PATH > /dev/null
(echo -e "4\n4\n3\n${CLD_PROVIDER}" | make deploy) && sleep 5

# test
make aisfs cli
BUCKET=${BUCKET} make test-long
EXIT_CODE=$?
popd > /dev/null

exit ${EXIT_CODE}
