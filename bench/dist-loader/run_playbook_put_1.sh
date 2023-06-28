#!/bin/bash

ansible-playbook -i inventory.yaml playbooks/benchmark.yaml -f 10 --become -e "ansible_become_pass=y grafana_host=dgx5826 bench_type=put bench_size=1MB total_size=1GB"