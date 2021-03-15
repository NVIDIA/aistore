#!/bin/bash

set -e

root_dir="$GOPATH/src/github.com/NVIDIA/aistore"

# Default values
aws_provider="n"
azure_provider="n"
gcp_provider="n"
hdfs_provider="n"
loopback="n"

targets=5
proxies=5
mpoints=5
next_tier=""

export MODE="debug" # By default start in debug mode
export AIS_NODE_FLAGS="-skip_startup"

while (( "$#" )); do
  case "${1}" in
    --aws)   aws_provider="y";   shift;;
    --azure) azure_provider="y"; shift;;
    --gcp)   gcp_provider="y";   shift;;
    --hdfs)  hdfs_provider="y";  shift;;
    --loopback)  loopback="y";  shift;;

    --dir) root_dir=$2; shift; shift;;
    --debug) export AIS_DEBUG=$2; shift; shift;;
    --tier) next_tier="true"; shift;;
    --ntargets) targets=$2; shift; shift;;
    --nproxies) proxies=$2; shift; shift;;
    --mountpoints) mpoints=$2; shift; shift;;
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

echo -e "${targets}\n${proxies}\n${mpoints}\n${aws_provider}\n${gcp_provider}\n${azure_provider}\n${hdfs_provider}\n${loopback}\n" | make deploy

make aisfs cli

if [[ -n ${next_tier} ]]; then
  DEPLOY_AS_NEXT_TIER="true" make deploy <<< $'1\n1\n2\nn\nn\nn\nn\nn\n'
  sleep 4
  if [[ -z ${AIS_USE_HTTPS} ]]; then
    ais cluster attach alias=http://127.0.0.1:11080
  else
    ais cluster attach alias=https://127.0.0.1:11080
  fi
fi

popd
