#!/bin/bash

BUCKET=docker_local_bucket
AISTORE_PATH=$GOPATH/src/github.com/NVIDIA/aistore
DOCKER_DATA_DIR="/data"

if [[ ${CLD_PROVIDER} == 1 ]]; then
    BUCKET=${HOSTNAME}
    aws s3api create-bucket --bucket ${BUCKET} --region ${AWS_DEFAULT_REGION} --create-bucket-configuration LocationConstraint=${AWS_DEFAULT_REGION}
elif [[ ${CLD_PROVIDER} == 2 ]]; then
    BUCKET=smth # TODO:
fi

function cleanup {
    if [[ ${CLD_PROVIDER} == 1 ]]; then
        aws s3 rb s3://${BUCKET} --force
    elif [[ ${CLD_PROVIDER} == 2 ]]; then
        : # TODO: currently noop
    fi
}
trap cleanup EXIT

pushd $AISTORE_PATH > /dev/null
(echo -e "4\n4\n3\n${CLD_PROVIDER}" | make deploy) && make cli && sleep 5

pushd python/tar2tf > /dev/null
ais create bucket tar-bucket
if [[ -d $DOCKER_DATA_DIR ]]; then
   find $DOCKER_DATA_DIR -type f -regex ".*\(.tar.gz\|.tar\|.tar.xz\|.tgz\|.txz\)" -exec ais put {} ais://tar-bucket --progress --verbose \;
 else
   ais show download $(ais start download "gs://lpr-imagenet/imagenet_train-{0000..0002}.tgz" ais://tar-bucket) --progress
fi
source /venv/bin/activate && jupyter lab --port=8888 --no-browser --ip=0.0.0.0 --allow-root
popd > /dev/null

popd > /dev/null

exit
