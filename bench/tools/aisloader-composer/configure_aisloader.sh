#!/bin/bash

ansible-playbook -i inventory.yaml playbooks/aisloader_config.yaml --become -e "ansible_become_pass=y"