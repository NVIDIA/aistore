#!/bin/bash

source common.sh

PLAYBOOK=playbooks/start_grafana.yaml

run_ansible_playbook "$PLAYBOOK"