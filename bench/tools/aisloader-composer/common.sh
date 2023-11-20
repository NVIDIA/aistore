#!/bin/bash

INVENTORY="inventory/oci.yaml"
GRAFANA_HOST="aismonitor"

# Hide output for tasks we aren't executing
export ANSIBLE_DISPLAY_SKIPPED_HOSTS=false

run_ansible_playbook() {
    local playbook="$1"
    # Any extra variables to pass to the playbook
    local env="$2"
    ansible-playbook -i "$INVENTORY" "$playbook" -f 10 --become -e "ansible_become_pass=y grafana_host=$GRAFANA_HOST $2"
}
