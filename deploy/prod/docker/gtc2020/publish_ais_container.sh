#!/usr/bin/env bash

#
# Publish aisnode-gtc:latest to docker hub
#

TAG=$1
if [[ -z "$TAG" ]]; then
    echo "No repo tag specified" >&2
    exit 2
fi

REPO=gmaltby/aisnode-gtc:${TAG}

docker tag aisnode-gtc:latest $REPO
docker push $REPO
