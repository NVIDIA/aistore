#!/bin/bash

echo 'Add GOPATH and GOBIN'
sudo touch /etc/profile.d/dfcpaths.sh
sudo sh -c "echo export PATH=$PATH:/usr/local/go/bin > /etc/profile.d/dfcpaths.sh"
sudo sh -c "echo export GOBIN=$HOME/dfc/bin >> /etc/profile.d/dfcpaths.sh"
sudo sh -c "echo export GOPATH=$HOME/dfc/ >> /etc/profile.d/dfcpaths.sh"
sudo sh -c "echo export DFCSRC=$HOME/dfc/src/github.com/NVIDIA/dfcpub/dfc >> /etc/profile.d/dfcpaths.sh"
sudo chmod 777 /etc/profile.d/dfcpaths.sh
. /etc/profile.d/dfcpaths.sh

echo 'Download go'
curl -LO https://storage.googleapis.com/golang/go1.9.linux-amd64.tar.gz
shasum -a 256 go1.*
sudo tar -C /usr/local -xvzf go1.9.linux-amd64.tar.gz > /dev/null
sudo rm -rf go1.9.linux-amd64.tar.gz


