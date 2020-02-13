#!/usr/bin/env bash

#
# Publish ais-binaries to docker hub
#

TAG=$1
if [[ -z "$TAG" ]]; then
    echo "No repo tag version specified" >&2
    exit 2
fi

for distro in alpine ubuntu; do
    REPO=gmaltby/ais-binaries:${distro}-${TAG}
    docker tag ais-binaries:${distro} $REPO
    echo "Pushing $REPO ..."
    docker push $REPO
done
