#!/bin/bash

# Remove the config file before each installation
CONFIG_FILE="$XDG_CONFIG_HOME/ais/config.json"
if [[ -z "$XDG_CONFIG_HOME" ]]; then
    CONFIG_FILE="$HOME/.config/ais/config.json"
fi
if [[ -f "$CONFIG_FILE" ]]; then
    rm ${CONFIG_FILE}
fi

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_SCRIPT_DIR="${DIR}/autocomplete"
AUTOCOMPLETE_INSTALL_SCRIPT="${AUTOCOMPLETE_SCRIPT_DIR}/install.sh"

if [[ $1 == "--ignore-autocomplete" ]]; then
    exit 0
fi

bash ${AUTOCOMPLETE_INSTALL_SCRIPT}