#!/bin/bash

# Check if fusermount is installed on the system
fusermount -V >/dev/null 2>&1
if [[ $? -ne 0 ]]; then
    echo "fusermount not found."
    exit 1
fi

BIN=aisfs

# Remove existing binary file
FILE=${GOPATH}/bin/${BIN}
if [[ -x "${FILE}" ]]; then
    rm ${FILE}
fi

VERSION="0.2"
BUILD=$(git rev-parse --short HEAD)
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

go install -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" ${DIR}/*.go

if [[ $? -eq 0 ]]; then
    echo "Executable ${BIN} has been successfully installed."
else
    echo "Failed to install ${BIN}."
    exit 1
fi
