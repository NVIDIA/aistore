#!/bin/bash

INVENTORY="inventory/inventory.yaml"
GRAFANA_HOST="dgx5826"

run_ansible_playbook() {
    local playbook="$1"
    # Any extra variables to pass to the playbook
    local env="$2"
    ansible-playbook -i "$INVENTORY" "$playbook" -f 10 --become -e "ansible_become_pass=y grafana_host=$GRAFANA_HOST $2"
}