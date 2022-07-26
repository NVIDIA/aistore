#!/usr/bin/env bash

TAG=${1:-aistore/cluster-minimal:latest}

DIR=$(cd "$(dirname "$0")"; pwd -P)
AISTORE_PATH=$(git rev-parse --show-toplevel)

function cleanup {
  rm -rf aisnode*
  rm -rf limits.conf
}
trap cleanup INT TERM EXIT

cp ${AISTORE_PATH}/deploy/dev/local/aisnode_config.sh aisnode_config.sh
cp ${AISTORE_PATH}/deploy/conf/limits.conf limits.conf

docker image build \
    --tag ${TAG} \
    --ulimit nofile=1000000:1000000 \
    --compress \
    -f ${DIR}/Dockerfile \
    .
