#!/bin/bash
set -e
. /etc/profile.d/aispaths.sh
echo 'Pull latest AIS'
cd $AISSRC
git fetch
git reset --hard origin/master
git status
if [ ! -z $1 ]; then
    echo Git checkout branch $1
    git checkout $1
fi
BUILD=`git rev-parse --short HEAD`
go build && go install && GOBIN=$GOPATH/bin go install -tags="${CLDPROVIDER}" -ldflags "-X github.com/NVIDIA/aistore/ais.build=$BUILD" setup/ais.go
