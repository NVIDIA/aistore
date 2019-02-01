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

VERSION=`git rev-parse --short HEAD`
BUILD=`date +%FT%T%z`
echo "Cloud provider set to: ${CLDPROVIDER}"
GOBIN=$GOPATH/bin go install -tags="${CLDPROVIDER}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/ais.go
