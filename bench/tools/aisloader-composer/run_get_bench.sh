#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
DURATION="${AISLOADER_DURATION:-"0"}"
EPOCHS="${AISLOADER_EPOCHS:-0}"
BUCKET="${AISLOADER_BUCKET:-"bench_1MB"}"
OBJECT_LIST="${AISLOADER_OBJECTS:-""}"

run_ansible_playbook "$PLAYBOOK" "bench_type=get duration=$DURATION epochs=$EPOCHS bucket=$BUCKET filelist=$OBJECT_LIST"
