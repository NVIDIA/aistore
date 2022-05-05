#!/bin/bash

set -e

echo "Add GOPATH and GOBIN"

sudo touch /etc/profile.d/aispaths.sh
sudo sh -c 'cat << EOF > /etc/profile.d/aispaths.sh
export GOBIN=$HOME/ais/bin
export GOPATH=$HOME/ais
export PATH=$PATH:/usr/local/go/bin:$GOBIN
export AISSRC=$HOME/ais/src/github.com/NVIDIA/aistore/ais
export AISTORE_SRC=$HOME/ais/src/github.com/NVIDIA/aistore
EOF'

sudo chmod 777 /etc/profile.d/aispaths.sh
. /etc/profile.d/aispaths.sh

sudo rm -rf ~/ais || true
mkdir -p ~/ais/{bin,pkg,src}

GOLANG_VER_FILE="/usr/local/go/VERSION"
GOLANG_VERSION="go1.18"
CURRENT_GOLANG_VERSION=""
if [[ -f ${GOLANG_VER_FILE} ]]; then
  CURRENT_GOLANG_VERSION=$(cat ${GOLANG_VER_FILE})
fi

if [[ ${CURRENT_GOLANG_VERSION} != "${GOLANG_VERSION}" ]]; then
  echo "Current Golang version does not match with expected, so updating Golang to " ${GOLANG_VERSION}
  sudo rm -rf /usr/local/go /usr/bin/go
  echo "Downloading Go..."
  curl -LO https://storage.googleapis.com/golang/${GOLANG_VERSION}.linux-amd64.tar.gz
  shasum -a 256 go1.*
  sudo tar -C /usr/local -xvzf ${GOLANG_VERSION}.linux-amd64.tar.gz >/dev/null
  sudo ln -s /usr/local/go/bin/go /usr/bin/go
  rm -rf ${GOLANG_VERSION}.linux-amd64.tar.gz
fi

GIT_NVIDIA=${GOPATH}/src/github.com/NVIDIA
mkdir -p "${GIT_NVIDIA}"
cd "${GIT_NVIDIA}"
git clone https://github.com/NVIDIA/aistore.git

echo "Backend provider(s) set to: ${AIS_BACKEND_PROVIDERS}"
cd aistore && make node
