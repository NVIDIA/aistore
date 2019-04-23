#!/bin/bash

# Remove the old version
FILE=$GOPATH/bin/ais
if [[ -x "$FILE" ]]; then
    rm $FILE
fi

VERSION="0.2"
BUILD=`git rev-parse --short HEAD`

# Install the CLI and enable auto-complete
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
GOBIN=$GOPATH/bin go install -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" $DIR/ais.go