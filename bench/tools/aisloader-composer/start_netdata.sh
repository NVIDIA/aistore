#!/bin/bash

source common.sh

PLAYBOOK=playbooks/netdata.yaml
CLUSTER_NAME=ais

run_ansible_playbook "$PLAYBOOK" "cluster_name=$CLUSTER_NAME"