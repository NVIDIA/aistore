#!/bin/bash

set -e

for node in $(kubectl get nodes -o name); do
    export NODE_NAME="${node#node/}"
    export PARENT_DIR="/mnt/data"
    export HOST_PATH="/mnt/data/ais"
    export LOG_PARENT_DIR="/ais"
    export LOG_PATH="/ais/log"
    export JOB_NAME="node-cleanup-$NODE_NAME"
    envsubst < ./utils/cleanup_job.tpl.yaml | kubectl apply -f -
done
