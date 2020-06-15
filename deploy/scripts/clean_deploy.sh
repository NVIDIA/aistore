#!/bin/bash

set -e

CLOUD=0 # By default use no cloud
AIS_DIR="$GOPATH/src/github.com/NVIDIA/aistore"
NEXT_TIER=""

export MODE="debug" # By default start in debug mode
export AISNODE_FLAGS="-skip_startup"

while (( "$#" )); do
  case "${1}" in
    --cloud) CLOUD=$2; shift; shift;;
    --dir) AIS_DIR=$2; shift; shift;;
    --debug) export AIS_DEBUG=$2; shift; shift;;
    --tier) NEXT_TIER="true"; shift;;
    --https)
      export USE_HTTPS="true"
      export AIS_SKIP_VERIFY_CRT="true"
      export AIS_HTTPS_CERT="$HOME/localhost.crt"
      export AIS_HTTPS_KEY="$HOME/localhost.key"
      shift
      ;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

pushd $AIS_DIR

make kill
make clean
make rmcache

echo -e "5\n5\n5\n${CLOUD}" | make deploy

make aisfs && make cli

if [[ -n $NEXT_TIER ]]; then
  DEPLOY_AS_NEXT_TIER="true" make deploy <<< $'1\n1\n2\n0'
  sleep 4
  if [[ -z $USE_HTTPS ]]; then
    ais attach remote alias=http://127.0.0.1:11080
  else
    ais attach remote alias=https://127.0.0.1:11080
  fi
fi

popd
