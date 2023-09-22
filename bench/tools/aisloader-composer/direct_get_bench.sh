#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
DURATION="${AISLOADER_DURATION:-"1m"}"
S3_ENDPOINT="${AISLOADER_S3_ENDPOINT:-"https://pbss.s8k.io"}"
BUCKET="${AISLOADER_BUCKET:-"ais-throughput-test-replica"}"
OBJECT_LIST="${AISLOADER_OBJECTS:-""}"


run_ansible_playbook "$PLAYBOOK" "bench_type=direct_get duration=$DURATION s3_endpoint=$S3_ENDPOINT bucket=$BUCKET filelist=$OBJECT_LIST direct"

