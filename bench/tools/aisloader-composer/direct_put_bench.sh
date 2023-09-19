#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark_direct.yaml
BENCH_SIZE="10MB"
TOTAL_SIZE="1GB"
S3_ENDPOINT="https://pbss.s8k.io"
CLOUD_BUCKET="ais-replica-10MB"

run_ansible_playbook "$PLAYBOOK" "bench_type=put bench_size=$BENCH_SIZE total_size=$TOTAL_SIZE s3_endpoint=$S3_ENDPOINT bucket=$CLOUD_BUCKET"
