#!/bin/bash

set -e
INIT_DIR=$(pwd)
echo 'Add GOPATH and GOBIN'
export PATH=$PATH:/usr/local/go/bin 
export GOBIN=$HOME/ais/bin 
export GOPATH=$HOME/ais/ 
export AISSRC=$HOME/ais/src/github.com/NVIDIA/aistore/ais 

echo 'Go get AIS'
rm -rf ~/ais || true
mkdir -p ~/ais/{bin,pkg,src}

echo 'Setup go dep binary'
curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh

echo 'Go get AIStore source'
cd $GOPATH/src
/usr/local/go/bin/go get -v github.com/NVIDIA/aistore/ais
cd $AISSRC

VERSION=`git rev-parse --short HEAD`
BUILD=`date +%FT%T%z`
echo "Cloud provider set to: ${CLDPROVIDER}"
GOBIN=$GOPATH/bin go install -tags="${CLDPROVIDER}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/ais.go

mkdir -p ${INIT_DIR}/ais_binary
cp ${GOBIN}/ais ${INIT_DIR}/ais_binary/ais
