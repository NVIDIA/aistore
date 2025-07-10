#!/bin/bash

# -----------------------------------------------------------------------------
# Copies the HTTPS certificate to the AISLoader hosts using an Ansible playbook.
#
# The script picks up the certificate from `playbooks/files/ca.crt`
# and places it at `/tmp/ca.crt` on each AISLoader host.
#
# Usage:
#   ./copy_https_cert.sh
# -----------------------------------------------------------------------------

set -euo pipefail

# Load shared functions and configurations
source common.sh

# Path to the Ansible playbook
PLAYBOOK="playbooks/copy_crt.yaml"

# Execute the Ansible playbook
run_ansible_playbook "$PLAYBOOK"
