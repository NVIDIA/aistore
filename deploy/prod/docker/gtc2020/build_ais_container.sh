#!/usr/bin/env bash

DIR=$(cd "$(dirname "$0")"; pwd -P)

if [[ $# -eq 0 ]]; then
    echo "Usage: $0 local|<tag>" >&2
    exit 2
fi

#
# If cmdline 'localbins' is present then try to use local
# ais binaries container as built by build_ais_binaries_container.sh,
# otherwise go to published repo.
#
if [[ "$1" == "local" ]]; then
    aisbin="ais-binaries:alpine"
else
    aisbin="gmaltby/ais-binaries:alpine-$1"
fi

# 
# Build the ais container image with context the current dir only -
# remain independent of the rest of the ais repo.
#
docker image build \
    --ulimit nofile=1000000:1000000 \
    --tag="aisnode-gtc:latest" \
    --compress \
    --build-arg AISBIN_IMAGE="$aisbin" \
    -f ${DIR}/Dockerfile-aiscontainer \
    ${DIR}