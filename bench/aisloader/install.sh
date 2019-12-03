#!/bin/bash

BUILD=$(git rev-parse --short HEAD)
GOBIN=${GOPATH}/bin go install -ldflags "-w -s -X main.build=${BUILD}"
