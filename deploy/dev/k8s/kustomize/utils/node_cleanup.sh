#!/bin/bash

for node in $(kubectl get nodes -o name); do
    export NODE_NAME="${node#node/}"
    export PARENT_DIR="/tmp"
    export HOST_PATH="/tmp/ais"
    export JOB_NAME="test-cleanup-$NODE_NAME"
    envsubst < ../kube_templates/cleanup_job_template.yml > cleanup_job.yml
    kubectl apply -f cleanup_job.yml
done
