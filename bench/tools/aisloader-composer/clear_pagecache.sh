#!/bin/bash

source common.sh

PLAYBOOK=playbooks/clear_cache.yaml

run_ansible_playbook "$PLAYBOOK"
