#!/bin/bash
set -e
echo 'Add GOPATH and GOBIN'
sudo touch /etc/profile.d/aispaths.sh
sudo sh -c "echo export PATH=$PATH:/usr/local/go/bin > /etc/profile.d/aispaths.sh"
sudo sh -c "echo export GOBIN=$HOME/ais/bin >> /etc/profile.d/aispaths.sh"
sudo sh -c "echo export GOPATH=$HOME/ais/ >> /etc/profile.d/aispaths.sh"
sudo sh -c "echo export AISSRC=$HOME/ais/src/github.com/NVIDIA/aistore/ais >> /etc/profile.d/aispaths.sh"
sudo chmod 777 /etc/profile.d/aispaths.sh
. /etc/profile.d/aispaths.sh
rm -rf ~/ais || true
mkdir -p ~/ais/{bin,pkg,src}

if [ ! -d "/usr/local/go" ]; then
    GOLANG_VERSION="1.12"

    echo 'Download go'
    curl -LO https://storage.googleapis.com/golang/go${GOLANG_VERSION}.linux-amd64.tar.gz
    shasum -a 256 go1.*
    sudo tar -C /usr/local -xvzf go${GOLANG_VERSION}.linux-amd64.tar.gz > /dev/null
    sudo ln -s /usr/local/go/bin/go /usr/bin/go
    rm -rf go${GOLANG_VERSION}.linux-amd64.tar.gz
fi
echo 'Setup go dep binary'
curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
echo 'Go get AIStore source'
cd $GOPATH/src
go get -v github.com/NVIDIA/aistore/ais
cd $AISSRC

VERSION=`git rev-parse --short HEAD`
BUILD=`date +%FT%T%z`
echo "Cloud provider set to: ${CLDPROVIDER}"
GODEBUG=madvdontneed=1 GOBIN=$GOPATH/bin go install -tags="${CLDPROVIDER}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/aisnode.go
