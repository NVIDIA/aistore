#!/bin/bash

ansible-playbook -i inventory.yaml playbooks/netdata.yaml --become -e "ansible_become_pass=y grafana_host=dgx5826"