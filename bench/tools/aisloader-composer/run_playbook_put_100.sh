#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
BENCH_SIZE="100MB"
TOTAL_SIZE="10GB"

run_ansible_playbook "$PLAYBOOK" "bench_type=put bench_size=$BENCH_SIZE total_size=$TOTAL_SIZE bucket=$BUCKET"
