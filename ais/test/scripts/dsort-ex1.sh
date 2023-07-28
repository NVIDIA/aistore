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
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## generate 10 tar shards, each containing 5 files
## (50 files total)
ais create $srcbck $dstbck 2>/dev/null
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

## _not_ to remove test buckets comment out the following 2 lines
echo "Cleanup: deleting $srcbck and $dstbck"
ais rmb $srcbck $dstbck -y 2>/dev/null 1>&2
