#!/usr/bin/env bash

DIR=$(cd "$(dirname "$0")"; pwd -P)
AISTORE_PATH=$(git rev-parse --show-toplevel)

function cleanup {
    rm -f ${AISTORE_PATH}/.dockerignore
}

#
# Build ais binaries container, tagged locally as ais-gtc:latest
# The ais container copies binaries from this container, defaulting
# to images published on docker hub.
#
echo ".git" > ${AISTORE_PATH}/.dockerignore # do not send Git as context to image
for distro in alpine ubuntu; do
    echo "Build for $distro ..."
    docker image build \
        --ulimit nofile=1000000:1000000 \
        --tag="ais-binaries:${distro}" \
        --build-arg AIS_VERSION=$(git rev-parse HEAD) \
        --compress \
        -f ${DIR}/Dockerfile-aisbin-${distro} \
        ${AISTORE_PATH}
done