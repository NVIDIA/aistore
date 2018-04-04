#!/bin/bash
. /etc/profile.d/dfcpaths.sh
echo 'Kill current DFC daemons, if any'
ps -C dfc -o pid= | xargs sudo kill -9
echo 'Go get DFC'
rm -rf ~/dfc
mkdir -p ~/dfc/{bin,pkg,src}
/usr/local/go/bin/go get -u -v github.com/NVIDIA/dfcpub/dfc
cd $DFCSRC
BUILD=`git rev-parse --short HEAD`
/usr/local/go/bin/go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" setup/dfc.go

