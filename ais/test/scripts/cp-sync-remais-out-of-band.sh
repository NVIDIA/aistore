#!/bin/bash

## Prerequisites: #################################################################################
# - cloud bucket
# - aistore cluster
# - remote aistore cluster (referred to as "remais")
#
## Example usage:
## ./ais/test/scripts/cp-sync-remais-out-of-band.sh --bucket s3://abc      ########################

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
  ais rmo $src --prefix $prf --wait
  exit $rc
}

trap cleanup EXIT INT TERM

ais archive gen-shards "$src/$prf/shard-{001..500}.tar" 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "1. initial"
ais cp $src $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo "2. remove 10 from the source"
ais rmo "$src/$prf/shard-{51..60}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "3. copy bucket w/ --sync"
ais cp $src $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo "4. remove another 10"
ais rmo "$src/$prf/shard-{151..160}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "5. copy --sync (bash-expansion defined) range, and compare"
ais cp "$src/$prf/shard-{001..500}.tar" $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }


#
# out of band DELETE using remais
#

echo "6. use remais to out-of-band remove 10 more from the source"
AIS_ENDPOINT=$rendpoint ais rmo "$src/$prf/shard-{251..260}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "7. copy bucket w/ --sync"
ais cp $src $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo "7.1. currently, when copying we always synchronize content of the in-cluster source as well"
cnt_src_cached=$(ais ls $src --prefix $prf --cached | grep Listed)
[[ $cnt_src_cached == $cnt_src ]] || { echo "FAIL: '$cnt_src_cached' != '$cnt_src'"; exit 1; }

echo "8. use remais to out-of-band remove the last 10 from the source"
AIS_ENDPOINT=$rendpoint ais rmo "$src/$prf/shard-{351..360}.tar" --wait 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "9. copy --sync (bash-expansion defined) range, and compare"
ais cp "$src/$prf/shard-{001..500}.tar" $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }

echo "9.1. currently, when copying we always synchronize content of the in-cluster source as well"
cnt_src_cached=$(ais ls $src --prefix $prf --cached | grep Listed)
[[ $cnt_src_cached == $cnt_src ]] || { echo "FAIL: '$cnt_src_cached' != '$cnt_src'"; exit 1; }

#
# out of band ADD using remais
#

echo "10. use remais to out-of-band add 17 new shards"
AIS_ENDPOINT=$rendpoint ais archive gen-shards "$src/$prf/shard-{501..517}.tar" 1>/dev/null 2>&1
cnt_src=$(ais ls $src --prefix $prf | grep Listed)

echo "10.1. copy --sync (bash-expansion defined) range, and compare"
echo "NOTE: currently, this part requires template-based copy !!!!!!!!!!!!!!!!!!!!" ## TODO -- FIXME
ais cp "$src/$prf/shard-{001..600}.tar" $dst --sync --wait 1>/dev/null 2>&1
cnt_dst=$(ais ls $dst --prefix $prf | grep Listed)
[[ $cnt_src == $cnt_dst ]] || { echo "FAIL: '$cnt_src' != '$cnt_dst'"; exit 1; }
