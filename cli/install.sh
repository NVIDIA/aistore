#!/bin/bash

# Remove the old version
FILE=${GOPATH}/bin/ais
if [[ -x "$FILE" ]]; then
    rm ${FILE}
fi

VERSION="0.3"
BUILD=`git rev-parse --short HEAD`
BINARY_NAME="ais"
URL="http://127.0.0.1:8080"
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

AUTOCOMPLETE_SCRIPT_DIR="${DIR}/autocomplete"
AUTOCOMPLETE_INSTALL_SCRIPT="${AUTOCOMPLETE_SCRIPT_DIR}/install.sh"

# Install the CLI
GOBIN=${GOPATH}/bin go install -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" ${DIR}/${BINARY_NAME}.go

if [[ "$?" -eq 0 ]]; then
    echo "*** AIS CLI successfully installed."
    echo "*** "

    # Install autocompletions
    bash ${AUTOCOMPLETE_INSTALL_SCRIPT}
else
    echo "Error installing AIS CLI."
fi
