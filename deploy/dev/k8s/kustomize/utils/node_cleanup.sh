#!/bin/bash

for node in $(kubectl get nodes -o name); do
    export NODE_NAME="${node#node/}"
    export PARENT_DIR="/tmp"
    export HOST_PATH="/tmp/ais"
    export LOG_PARENT_DIR="/ais"
    export LOG_PATH="/ais/log"
    export JOB_NAME="node-cleanup-$NODE_NAME"
    envsubst < ./utils/cleanup_job_template.yml | kubectl apply -f -
done
