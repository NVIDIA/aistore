#!/bin/bash

## Example usage:
## ./ais/test/scripts/dsort-ex1.sh --srcbck ais://aaaaa --dstbck ais://qqqqq

SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd -P)"

## Command line options and their respective defaults
srcbck="ais://src"
dstbck="ais://dst"

while (( "$#" )); do
  case "${1}" in
    --srcbck) srcbck=$2; shift; shift;;
    --dstbck) dstbck=$2; shift; shift;;
    --nocleanup) nocleanup="true"; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## establish existence
ais show bucket $srcbck -c 1>/dev/null 2>&1
srcexists=$?
ais show bucket $dstbck -c 1>/dev/null 2>&1
dstexists=$?

## generate 10 tar shards, each containing 5 files (50 files total)
## note that dstbck, if doesn't exist, will be created on the fly
[[ $srcexists == 0 ]] || ais create $srcbck || exit 1
ais archive gen-shards "$srcbck/shard-{0..9}.tar" || \
exit 1

## run dsort and wait for the job to finish
## (see 'dsort-ex1-spec.json' in this directory)
ais wait $(ais start dsort ${srcbck} ${dstbck} -f ${SCRIPT_PATH}/dsort-ex1-spec.json)

## list new shards to confirm 5 new shards, each containing 10 original files
## (the same 50 total - see above)
num=$(ais ls $dstbck --summary --H | awk '{print $3}')
[[ $num == 5 ]] || { echo "FAIL: $num != 5"; exit 1; }

echo "Successfully resharded $srcbck => $dstbck:"
ais ls $dstbck

## cleanup: rmb buckets created during this run
if [[ ${nocleanup} != "true" && $srcexists != 0 ]]; then
  echo "Deleting source: $srcbck"
  ais rmb $srcbck -y 2>/dev/null 1>&2
fi
if [[ ${nocleanup} != "true" && $dstexists != 0 ]]; then
  echo "Deleting destination: $dstbck"
  ais rmb $dstbck -y 2>/dev/null 1>&2
fi
