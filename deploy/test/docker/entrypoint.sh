#!/bin/bash

BUCKET=docker_local_bucket
AIS=$GOPATH/src/github.com/NVIDIA/aistore

if [ ${CLD_PROVIDER} == 1 ]; then
    BUCKET=${HOSTNAME}
    aws s3api create-bucket --bucket ${BUCKET} --region ${AWS_DEFAULT_REGION} --create-bucket-configuration LocationConstraint=${AWS_DEFAULT_REGION}
elif [ ${CLD_PROVIDER} == 2 ]; then
    BUCKET=smth # TODO:
fi

function cleanup {
    if [ ${CLD_PROVIDER} == 1 ]; then
        aws s3 rb ${BUCKET} --force
    elif [ ${CLD_PROVIDER} == 2 ]; then
        : # TODO: currently noop
    fi
}
trap cleanup EXIT INT TERM

pushd $AIS > /dev/null

# try to build and deploy
pushd $AIS/ais > /dev/null
go build -tags="aws" setup/ais.go && go build -tags="gcp" setup/ais.go && go build -tags="" setup/ais.go && rm -rf ais
echo -e "4\n4\n3\n${CLD_PROVIDER}" > deploy.tmp && make deploy < deploy.tmp && sleep 5
popd > /dev/null

# test
BUCKET=${BUCKET} gotest -v -p 1 -count 1 -timeout 1h ./...
EXIT_CODE=$?
popd > /dev/null

exit ${EXIT_CODE}
