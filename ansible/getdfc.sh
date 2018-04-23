#!/bin/bash
set -e
. /etc/profile.d/dfcpaths.sh
echo 'Pull latest DFC'
cd $DFCSRC
git pull
BUILD=`git rev-parse --short HEAD`
/usr/local/go/bin/go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" setup/dfc.go
