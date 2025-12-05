#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
DURATION="${AISLOADER_DURATION:-"20m"}"
EPOCHS="${AISLOADER_EPOCHS:-0}"
BUCKET="${AISLOADER_BUCKET:-"ais://ais-bench-10KiB"}"
OBJECT_LIST="${AISLOADER_OBJECTS:-"/path/to/object_list.txt"}"
GET_BATCHSIZE="${AISLOADER_GET_BATCHSIZE:-"16"}"


run_ansible_playbook "$PLAYBOOK" "bench_type=get_batch duration=$DURATION epochs=$EPOCHS bucket=$BUCKET filelist=$OBJECT_LIST batch_size=$GET_BATCHSIZE"
