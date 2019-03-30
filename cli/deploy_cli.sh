#!/bin/bash

# Install the CLI and enable auto-complete
go build -o ${GOPATH}/bin/ais ../../../cli
source ../../../cli/aiscli_autocomplete