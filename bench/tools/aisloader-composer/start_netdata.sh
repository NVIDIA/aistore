#!/bin/bash

source common.sh

PLAYBOOK=playbooks/netdata.yaml

run_ansible_playbook "$PLAYBOOK"