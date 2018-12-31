#!/bin/bash
set -e
. /etc/profile.d/dfcpaths.sh
echo 'Pull latest DFC'
cd $DFCSRC
git fetch
git reset --hard origin/master
git status
if [ ! -z $1 ]; then
    echo Git checkout branch $1
    git checkout $1
fi
BUILD=`git rev-parse --short HEAD`
go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" setup/dfc.go
