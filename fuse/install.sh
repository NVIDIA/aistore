#!/bin/bash

printError() {
  echo "Error: $1."
  exit 1
}

# Check if fusermount is installed on the system
OS=$(uname -s)
case $OS in
  Linux) # Linux
    fusermount -V >/dev/null 2>&1
    if [[ $? -ne 0 ]]; then
      printError "fusermount not found"
    fi
    ;;
  Darwin) # macOS
    which umount
    if [[ $? -ne 0 ]]; then
      printError "umount not found"
    fi
    echo "WARNING: Darwin architecture is not yet fully supported. You may stumble upon bugs and issues when testing on Mac."
    ;;
  *)
    printError "'${OS}' is not supported"
    ;;
esac

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
