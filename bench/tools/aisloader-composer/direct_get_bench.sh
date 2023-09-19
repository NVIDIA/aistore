#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark_direct.yaml
DURATION="1m"
S3_ENDPOINT="https://pbss.s8k.io"
CLOUD_BUCKET="ais-replica-10MB"

run_ansible_playbook "$PLAYBOOK" "bench_type=get duration=$DURATION s3_endpoint=$S3_ENDPOINT bucket=$CLOUD_BUCKET"

