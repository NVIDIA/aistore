#!/bin/bash
set -e
echo 'Add GOPATH and GOBIN'
sudo touch /etc/profile.d/dfcpaths.sh
sudo sh -c "echo export PATH=$PATH:/usr/local/go/bin > /etc/profile.d/dfcpaths.sh"
sudo sh -c "echo export GOBIN=$HOME/dfc/bin >> /etc/profile.d/dfcpaths.sh"
sudo sh -c "echo export GOPATH=$HOME/dfc/ >> /etc/profile.d/dfcpaths.sh"
sudo sh -c "echo export DFCSRC=$HOME/dfc/src/github.com/NVIDIA/dfcpub/dfc >> /etc/profile.d/dfcpaths.sh"
sudo chmod 777 /etc/profile.d/dfcpaths.sh
. /etc/profile.d/dfcpaths.sh
rm -rf ~/dfc || true
mkdir -p ~/dfc/{bin,pkg,src}

if [ ! -d "/usr/local/go" ]; then
    echo 'Download go'
    curl -LO https://storage.googleapis.com/golang/go1.11.linux-amd64.tar.gz
    shasum -a 256 go1.*
    sudo tar -C /usr/local -xvzf go1.11.linux-amd64.tar.gz > /dev/null
    sudo ln -s /usr/loca/go/bin/go /usr/bin/go
    rm -rf go1.11.linux-amd64.tar.gz
fi
echo 'Setup go dep binary'
curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
echo 'Go get DFC'
cd $GOPATH/src
go get -v github.com/NVIDIA/dfcpub/dfc
cd $DFCSRC
BUILD=`git rev-parse --short HEAD`
go build && go install && GOBIN=$GOPATH/bin go install -ldflags "-X github.com/NVIDIA/dfcpub/dfc.build=$BUILD" setup/dfc.go

