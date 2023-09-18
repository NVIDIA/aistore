#!/bin/bash

source common.sh

PLAYBOOK=playbooks/install_docker.yaml

run_ansible_playbook "$PLAYBOOK" "$INVENTORY"