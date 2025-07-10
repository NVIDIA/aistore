#!/bin/bash

# -----------------------------------------------------------------------------
# Copies the AuthN token to the AISLoader hosts using an Ansible playbook.
#
# The script picks up the token from `playbooks/files/auth.token`
# and places it at `/tmp/auth.token` on each AISLoader host.
#
# Usage:
#   ./copy_authn_token.sh
# -----------------------------------------------------------------------------

set -euo pipefail

# Load shared functions and configurations
source common.sh

# Path to the Ansible playbook
PLAYBOOK="playbooks/copy_authn_token.yaml"

# Execute the Ansible playbook
run_ansible_playbook "$PLAYBOOK"
