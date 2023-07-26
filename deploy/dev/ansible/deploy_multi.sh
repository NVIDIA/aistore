#!/bin/bash

# Run the deploy for all hosts in inventory.yaml 
ansible-playbook  -i inventory.yaml setup.yaml -e "ansible_become_pass=y" -vvv

# Deploy the primary proxy first, so targets can connect
ansible-playbook  -i inventory.yaml deploy_primary.yaml -e "ansible_become_pass=y" -vvv

# Wait for primary to be ready before adding more targets
sleep 10

# Deploy additional targets
ansible-playbook  -i inventory.yaml deploy_target.yaml -e "ansible_become_pass=y" -vvv

