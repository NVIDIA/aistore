#!/bin/bash

# Remove the old version
FILE=${GOPATH}/bin/ais
if [[ -x "$FILE" ]]; then
    rm ${FILE}
fi

# Remove the config file before each installation
CONFIG_FILE="$XDG_CONFIG_HOME/ais/config.json"
if [[ -z "$XDG_CONFIG_HOME" ]]; then
    CONFIG_FILE="$HOME/.config/ais/config.json"
fi
if [[ -f "$CONFIG_FILE" ]]; then
    rm ${CONFIG_FILE}
fi

VERSION="0.4"
BUILD=$(git rev-parse --short HEAD)
URL="http://127.0.0.1:8080"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_SCRIPT_DIR="${DIR}/autocomplete"
AUTOCOMPLETE_INSTALL_SCRIPT="${AUTOCOMPLETE_SCRIPT_DIR}/install.sh"

# Install the CLI
go install -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" ${DIR}/*.go

if [[ $? -eq 0 ]]; then
    echo "AIS CLI executable has been successfully installed."
    if [[ $1 == "--ignore-autocomplete" ]]; then
        exit 0
    fi

    bash ${AUTOCOMPLETE_INSTALL_SCRIPT}
else
    echo "Error installing AIS CLI"
    exit 1
fi
