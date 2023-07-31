#!/bin/bash

INVENTORY="inventory.yaml"
GRAFANA_HOST="dgx5826"
BENCH_SIZE="100MB"
DURATION="5m"

ansible-playbook -i $INVENTORY playbooks/benchmark.yaml -f 10 --become -e "ansible_become_pass=y grafana_host=$GRAFANA_HOST bench_type=get bench_size=$BENCH_SIZE duration=$DURATION"
