#!/bin/bash
set -e
. /etc/profile.d/dfcpaths.sh
echo 'Go get DFC'
rm -rf ~/dfc || true
mkdir -p ~/dfc/{bin,pkg,src}
/usr/local/go/bin/go get -u -v github.com/NVIDIA/dfcpub/dfc
cd $DFCSRC
BUILD=`git rev-parse --short HEAD`
/usr/local/go/bin/go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" setup/dfc.go

