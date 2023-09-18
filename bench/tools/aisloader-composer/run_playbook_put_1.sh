#!/bin/bash

source common.sh

PLAYBOOK=playbooks/benchmark.yaml
BENCH_SIZE="1MB"
TOTAL_SIZE="10GB"


ansible-playbook -i $INVENTORY playbooks/benchmark.yaml -f 10 --become -e "ansible_become_pass=y grafana_host=$GRAFANA_HOST bench_type=put bench_size=$BENCH_SIZE total_size=$TOTAL_SIZE"
