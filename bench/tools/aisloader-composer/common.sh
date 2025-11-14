#!/bin/bash

INVENTORY="inventory/oci.yaml"
GRAFANA_HOST="aismonitor"

run_ansible_playbook() {
    local playbook="$1"
    # Any extra variables to pass to the playbook
    local env="$2"
    ansible-playbook -i "$INVENTORY" "$playbook" --become -e "grafana_host=$GRAFANA_HOST $2"
}
