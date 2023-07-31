#!/bin/bash

ansible-playbook -i inventory.yaml playbooks/kill_aisloader.yaml --become -e "ansible_become_pass=y"