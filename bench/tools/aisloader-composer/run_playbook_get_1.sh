#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
BENCH_SIZE="1MB"
DURATION="1m"
BUCKET="bench_1MB"

run_ansible_playbook "$PLAYBOOK" "bench_type=get bench_size=$BENCH_SIZE duration=$DURATION bucket=$BUCKET"
