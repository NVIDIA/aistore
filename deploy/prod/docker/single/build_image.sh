#!/usr/bin/env bash

TAG=${1:-aistore/aistore-single-node:latest}

DIR=$(cd "$(dirname "$0")"; pwd -P)
AISTORE_PATH=$(git rev-parse --show-toplevel)

function cleanup {
  rm -rf aisnode_*
}
trap cleanup INT TERM EXIT

cp ${AISTORE_PATH}/deploy/dev/local/aisnode_config.sh aisnode_config.sh

# Build `aisnode` binaries statically so that it can be executed without any additions.
# https://blog.codeship.com/building-minimal-docker-containers-for-go-applications
for provider in "" "aws" "gcp" "azure"; do
  env \
    CLDPROVIDER=${provider} \
    GOOS="linux" GOARCH="amd64" CGO_ENABLED=0 BUILD_FLAGS="-a -installsuffix cgo" \
    make -C ${AISTORE_PATH} node

  mv ${GOPATH}/bin/aisnode aisnode_${provider}
done

docker image build \
    --tag ${TAG} \
    --ulimit nofile=1000000:1000000 \
    --tag="aistore-single-node" \
    --compress \
    -f ${DIR}/Dockerfile \
    .
