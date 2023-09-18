#!/bin/bash

source common.sh

PLAYBOOK=playbooks/aisloader_setup.yaml
AISLOADER_PATH="/path/to/aisloader"

run_ansible_playbook "$PLAYBOOK" "aisloader_path=$AISLOADER_PATH"