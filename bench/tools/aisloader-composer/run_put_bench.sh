#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
BENCH_SIZE="${AISLOADER_PUT_SIZE:-"1MB"}"
TOTAL_SIZE="${AISLOADER_TOTAL_SIZE:-"10G"}"
BUCKET="${AISLOADER_BUCKET:-"bench_1MB"}"

run_ansible_playbook "$PLAYBOOK" "bench_type=put bench_size=$BENCH_SIZE total_size=$TOTAL_SIZE bucket=$BUCKET"
