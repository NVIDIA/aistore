#!/bin/bash

set -e

root_dir="$GOPATH/src/github.com/NVIDIA/aistore"

# Default values
cloud="n\nn\nn\nn\n" # no cloud by default
targets=5
proxies=5
next_tier=""

export MODE="debug" # By default start in debug mode
export AIS_NODE_FLAGS="-skip_startup"

while (( "$#" )); do
  case "${1}" in
    --cloud) cloud="y\ny\nn\ny\n"; shift;;
    --dir) root_dir=$2; shift; shift;;
    --debug) export AIS_DEBUG=$2; shift; shift;;
    --tier) next_tier="true"; shift;;
    --ntargets) targets=$2; shift; shift;;
    --nproxies) proxies=$2; shift; shift;;
    --https)
      export AIS_USE_HTTPS="true"
      export AIS_SKIP_VERIFY_CRT="true"
      export AIS_SERVER_CRT="$HOME/localhost.crt"
      export AIS_SERVER_KEY="$HOME/localhost.key"
      shift
      ;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

pushd ${root_dir}

make kill
make clean

echo -e "${targets}\n${proxies}\n5\n${cloud}\nn" | make deploy

make aisfs && make cli

if [[ -n ${next_tier} ]]; then
  DEPLOY_AS_NEXT_TIER="true" make deploy <<< $'1\n1\n2\n0'
  sleep 4
  if [[ -z ${AIS_USE_HTTPS} ]]; then
    ais attach remote alias=http://127.0.0.1:11080
  else
    ais attach remote alias=https://127.0.0.1:11080
  fi
fi

popd
