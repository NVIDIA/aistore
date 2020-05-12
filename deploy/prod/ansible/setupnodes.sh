#!/bin/bash

set -e

echo "Add GOPATH and GOBIN"

sudo touch /etc/profile.d/aispaths.sh
sudo sh -c "echo export PATH=$PATH:/usr/local/go/bin > /etc/profile.d/aispaths.sh"
sudo sh -c "echo export GOBIN=$HOME/ais/bin >> /etc/profile.d/aispaths.sh"
sudo sh -c "echo export GOPATH=$HOME/ais/ >> /etc/profile.d/aispaths.sh"
sudo sh -c "echo export AISSRC=$HOME/ais/src/github.com/NVIDIA/aistore/ais >> /etc/profile.d/aispaths.sh"
sudo sh -c "echo export AISTORE_SRC=$HOME/ais/src/github.com/NVIDIA/aistore >> /etc/profile.d/aispaths.sh"
sudo chmod 777 /etc/profile.d/aispaths.sh
. /etc/profile.d/aispaths.sh

rm -rf ~/ais || true
mkdir -p ~/ais/{bin,pkg,src}

GOLANG_VERSION="go1.13"
CURRENT_GOLANG_VERSION=$(cat /usr/local/go/VERSION)
if [[ ${CURRENT_GOLANG_VERSION} != ${GOLANG_VERSION} ]]; then
    echo "Current Golang version does not match with expected, so updating Golang to " ${GOLANG_VERSION}
    sudo rm -rf /usr/local/go
    sudo rm -rf /use/bin/go
    echo "Downloading Go..."
    curl -LO https://storage.googleapis.com/golang/${GOLANG_VERSION}.linux-amd64.tar.gz
    shasum -a 256 go1.*
    sudo tar -C /usr/local -xvzf ${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null
    sudo ln -s /usr/local/go/bin/go /usr/bin/go
    rm -rf ${GOLANG_VERSION}.linux-amd64.tar.gz
fi

echo "Getting AIStore source..."
go get -v github.com/NVIDIA/aistore

echo "Cloud provider set to: ${AIS_CLD_PROVIDER}"
cd ${AISTORE_SRC} && make node
