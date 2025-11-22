#!/bin/bash

INVENTORY="inventory/oci.yaml"
GRAFANA_HOST="aismonitor"

run_ansible_playbook() {
    local playbook="$1"
    local extra_vars="${2:+ $2}"  # Prepend space only if $2 exists
    
    ansible-playbook -i "$INVENTORY" "$playbook" --become \
        -e "grafana_host=$GRAFANA_HOST$extra_vars"
}
