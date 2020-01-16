#!/usr/bin/env bash

TAG=${1:-aistore/aistore-single-node:latest}

DIR=$(cd "$(dirname "$0")"; pwd -P)
AISTORE_PATH=$(git rev-parse --show-toplevel)

function cleanup {
  rm -rf ${AISTORE_PATH}/.dockerignore
}
trap cleanup INT TERM EXIT

echo ".git" > ${AISTORE_PATH}/.dockerignore # do not send Git as context to image
docker image build \
    --tag ${TAG} \
    --ulimit nofile=1000000:1000000 \
    --tag="aistore-single-node" \
    --compress \
    -f ${DIR}/Dockerfile \
    ${AISTORE_PATH} # should we build from source or `github.com/NVIDIA/aistore:TAG`?
