#!/bin/bash

# e.g. usage:
# 1) ais/test/scripts/force-join.sh
# 2) ais/test/scripts/force-join.sh --smap --config --bmd

AISTORE_PATH=$(cd "$(dirname "$0")/../../../"; pwd -P)
# echo $AISTORE_PATH

while (( "$#" )); do
  case "${1}" in
    --config) config="true"; shift;;
    --bmd) bmd="true"; shift;;
    --smap) smap="true"; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

make kill clean
./scripts/clean_deploy.sh --target-cnt 6 --proxy-cnt 6 --mountpath-cnt 4 --deployment all --debug --aws --gcp --azure >/dev/null

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi
if ! [ -x "$(command -v aisloader)" ]; then
  echo "Error: aisloader not installed" >&2
  exit 1
fi

psi=$(ais show cluster proxy | grep "\[P\]" | awk '{print $1}')
pid=${psi:2:8}
echo "designated (destination) primary: $pid"

set -x
sleep 8
set +x

export AIS_ENDPOINT=http://127.0.0.1:11080 ##################################
echo "AIS_ENDPOINT=$AIS_ENDPOINT"

if [[ ${config} == "true" ]]; then
  for i in {1..10}; do
    ais config cluster lru.enabled true >/dev/null
    ais config cluster lru.enabled false >/dev/null
  done
  echo "victim config:"
  ais show cluster config --json | tail -3
fi

if [[ ${bmd} == "true" ]]; then
  ais create ais://nnn
  for i in {1..10}; do
    ais create "ais://nn$i" >/dev/null
  done
  echo "victim bmd:"
  ais show cluster bmd -H
fi


if [[ ${smap} == "true" ]]; then
  tsi=$(ais show cluster -H target | awk '{print $1}')
  for i in {1..10}; do
    ais cluster add-remove-nodes start-maintenance $tsi --yes >/dev/null
    ais cluster add-remove-nodes stop-maintenance $tsi --yes >/dev/null
  done
  echo "victim smap:"
  ais show cluster smap --json | tail -4
fi

sleep 1

ais cluster set-primary $pid http://127.0.0.1:8080 --force ##################################

sleep 1
unset -v AIS_ENDPOINT
echo "AIS_ENDPOINT=$AIS_ENDPOINT"
aisloader -bucket=ais://nnn -cleanup=false -numworkers=8 -quiet -pctput=100 -minsize=4K -maxsize=4K --duration 20s

find /tmp/ais_next -type f | grep "mp[1-4].*/1/" | wc -l

if [[ ${config} == "true" ]]; then
  echo "resulting config:"
  ais show cluster config --json | tail -3
fi
if [[ ${bmd} == "true" ]]; then
  echo "resulting bmd:"
  ais show cluster bmd --json | tail -3
fi

echo "resulting smap:"
ais show cluster | grep 'Proxies\|Targets'
ais show cluster smap --json | tail -4


ais cluster decommission --yes
