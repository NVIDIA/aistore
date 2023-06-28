#!/bin/bash

ansible-playbook -i inventory.yaml playbooks/start_grafana.yaml --become -e "ansible_become_pass=y grafana_host=dgx5826"