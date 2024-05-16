#!/bin/bash

## Prerequisites: #################################################################################
# - cloud bucket
# - aistore cluster
# - remote aistore cluster (referred to by its "remais" alias)
#
## Usage:
## cp-sync-remais-out-of-band.sh --bucket BUCKET       ### NOTE: requires cloud bucket
##
## Example:
## cp-sync-remais-out-of-band.sh --bucket s3://abc      ########################

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi

## default cloud bucket
src="s3://abc"

## constants
host="--host=s3.amazonaws.com"

while (( "$#" )); do
  case "${1}" in
    --bucket) src=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## uncomment for verbose output
## set -x

## establish existence
ais show bucket $src -c 1>/dev/null || exit $?

## remais must be attached
rendpoint=$(ais show remote-cluster -H | awk '{print $2}')
[[ ! -z "$rendpoint" ]] || { echo "Error: need remote ais cluster to make out-of-band changes"; exit 1; }

## remais must see the source as well
AIS_ENDPOINT=$rendpoint ais show bucket $src -c 1>/dev/null || exit $?

## temp vars
dst="ais://dst-$RANDOM" ## destination bucket (created on the fly)
prf="prefix-$RANDOM"    ## source prefix

## remember existing state

## cleanup
cleanup() {
  rc=$?
  ais rmo $src --prefix $prf --wait 1>/dev/null
  ais rmb $dst --yes 1>/dev/null
  exit $rc
}

trap cleanup EXIT INT TERM

echo " 1. generate and write 500 random shards => $src"
ais archive gen-shards "$src/$prf/shard-{001..500}.tar" 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo " 2. copy $src => $dst"
ais cp $src $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo " 3. remove 10 shards from the source"
ais rmo "$src/$prf/shard-{51..60}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo " 4. copy $src => $dst w/ synchronization ('--sync' option)"
ais cp $src $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo " 5. remove another 10 shards"
ais rmo "$src/$prf/shard-{151..160}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo " 6. copy multiple objects using bash-expansion defined range and '--sync'"
ais cp "$src/$prf/shard-{001..500}.tar" $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo " #"
echo " # out of band DELETE using remote AIS (remais)"
echo " #"

echo " 7. use remote AIS cluster (\"remais\") to out-of-band remove 10 shards from the source"
AIS_ENDPOINT=$rendpoint ais rmo "$src/$prf/shard-{251..260}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo " 8. copy $src => $dst w/ --sync"
ais cp $src $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo " 9. when copying, we always synchronize content of the in-cluster source as well"
cnt_src_cached=$(ais ls $src --prefix $prf --cached | grep Listed)
[[ $cnt_src_cached == $cnt_src ]] || { echo "FAIL: '$cnt_src_cached' != '$cnt_src'"; exit 1; }

echo "10. use remais to out-of-band remove 10 more shards from $src source"
AIS_ENDPOINT=$rendpoint ais rmo "$src/$prf/shard-{351..360}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "11. copy a range of shards from $src to $dst, and compare"
ais cp "$src/$prf/shard-{001..500}.tar" $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo "12. and again: when copying, we always synchronize content of the in-cluster source as well"
cnt_src_cached=$(ais ls $src --prefix $prf --cached | grep Listed)
[[ $cnt_src_cached == $cnt_src ]] || { echo "FAIL: '$cnt_src_cached' != '$cnt_src'"; exit 1; }

echo " #"
echo " # out of band ADD using remote AIS (remais)"
echo " #"

echo "13. use remais to out-of-band add (i.e., PUT) 17 new shards"
AIS_ENDPOINT=$rendpoint ais archive gen-shards "$src/$prf/shard-{501..517}.tar" 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "14. copy a range of shards from $src to $dst, and check whether the destination has new shards"
ais cp "$src/$prf/shard-{001..600}.tar" $dst --sync --wait 1>/dev/null 2>&1

echo "15. compare the contents but NOTE: as of v3.22, this part requires multi-object copy (using '--list' or '--template')"
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }
