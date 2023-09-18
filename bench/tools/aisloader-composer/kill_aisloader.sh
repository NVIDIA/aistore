#!/bin/bash

source common.sh

PLAYBOOK=playbooks/kill_aisloader.yaml

run_ansible_playbook "$PLAYBOOK"