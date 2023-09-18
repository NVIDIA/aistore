#!/bin/bash

source common.sh

PLAYBOOK=playbooks/aisloader_config.yaml

run_ansible_playbook "$PLAYBOOK"