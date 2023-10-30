#!/bin/bash

source common.sh

# Select playbooks/install_docker_centos.yaml for systems running CentOS,
# or use playbooks/install_docker.yaml for those with Ubuntu-based distributions.

PLAYBOOK=playbooks/install_docker.yaml

run_ansible_playbook "$PLAYBOOK" "$INVENTORY"