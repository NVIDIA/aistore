#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
BENCH_SIZE="${AISLOADER_PUT_SIZE:-"1MB"}"
TOTAL_SIZE="${AISLOADER_TOTAL_SIZE:-"1G"}"
S3_ENDPOINT="${AISLOADER_S3_ENDPOINT:-"https://pbss.s8k.io"}"
BUCKET="${AISLOADER_BUCKET:-"ais-replica-10MB"}"


run_ansible_playbook "$PLAYBOOK" "bench_type=direct_put bench_size=$BENCH_SIZE total_size=$TOTAL_SIZE s3_endpoint=$S3_ENDPOINT bucket=$BUCKET"
