#!/bin/bash

# Prerequisites: ########################################################################################
# - aistore cluster
# - ais (CLI)
#
## Example usage:
## 1. ./ais/test/scripts/get-archregx-wdskey.sh
## 2. ./ais/test/scripts/get-archregx-wdskey.sh --bck ais://abc
## 3. ./ais/test/scripts/get-archregx-wdskey.sh --bck ais://abc --dir /tmp/out
# #######################################################################################################

# command line

while (( "$#" )); do
  case "${1}" in
    --bck) bck=$2; shift; shift;;
    --dir) dir=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## runtime constants
#### random suffix & destination subdirectory
rand=$RANDOM

#### generate that many WD-formatted shards
num=32

## establish existence
bck_check_rc=1
if [ ! -z "$bck" ]; then
  ais show bucket $bck -c 1>/dev/null 2>&1
  bck_check_rc=$?
fi
dir_check_rc=1
if [ ! -z "$dir" ]; then
  ls $dir 1>/dev/null 2>&1
  dir_check_rc=$?
fi

if [[ $bck_check_rc != 0 ]]; then
  if [ -z "$bck" ]; then
    bck="ais://archregx-$rand"
  fi
  ais create $bck || exit 1
fi
if [[ $dir_check_rc != 0 ]]; then
  if [ -z "$dir" ]; then
    dir=$(mktemp -d -t archregx-XXX) || exit $?
  else
    mkdir $dir || exit $?
  fi
else
  dir="$dir/archregx-$rand"
  mkdir $dir || exit $?
fi

cleanup() {
  rc=$?
  if [[ $bck_check_rc == 0 ]]; then
    ais rmo $bck --prefix $rand 1>/dev/null
  else
    ais rmb $bck --yes 1>/dev/null
  fi
  rm -rf $dir 1>/dev/null

  [[ $rc == 0 ]] && echo "PASS: $num tests"
  exit $rc
}

trap cleanup EXIT INT TERM

## uncomment for verbose output
## set -x

ais archive gen-shards "$bck/$rand/shard-{01..$num}.tar" --fext '.mp3,.json,.cls'

shards=$(ais ls $bck --name-only | grep shard)

for s in $shards; do
  files=$(ais archive ls $bck/$s -name-only | grep /)
  for f in $files; do
     if [ "${f: -4}" == ".cls" ]; then
        name=$(basename -s .cls $f)
        ais archive get $bck/$s $dir/$name.tar --archregx=$name --archmode=wdskey 1>/dev/null
	cnt=$(tar tvf $dir/$name.tar | grep $name | wc -l)
	[[ $cnt == 3 ]] || { echo "FAIL: $cnt != 3"; exit 1; }
     fi
  done
done
