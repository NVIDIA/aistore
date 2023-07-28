#!/bin/bash

SCRIPT_PATH="$(cd "$(dirname "$0")"; pwd -P)"

## start from scratch and generate 10 tar shards, each containing 5 files
## (50 files total)
ais rmb ais://src ais://dst -y 2>/dev/null 1>&2
ais create ais://src ais://dst
ais archive gen-shards 'ais://src/shard-{0..9}.tar'

## run dsort and wait for the job to finish
## (see 'dsort-ex1-spec.json' in this directory)
ais wait $(ais start dsort -f ${SCRIPT_PATH}/dsort-ex1-spec.json)

## list new shards to confirm 5 new shards, each containing 10 original files
## (the same 50 total - see above)
num=$(ais ls ais://dst --summary --H | awk '{print $3}')
[[ $num == 5 ]] || { echo "FAIL: $num != 5"; exit 1; }

echo "Successfully resharded ais://src => ais://dst:"
ais ls ais://dst

echo
echo "Note: to remove test buckets, run: 'ais rmb ais://src ais://dst -y'"
