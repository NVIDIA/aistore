#!/bin/bash

## Prerequisites: #################################################################################
# - s3 bucket
# - s3cmd, $PATH-executable and configured to access the bucket out-of-band
# - aistore cluster, also configured to access the same bucket
#
## Usage:
## s3-cp-latest-prefix.sh --bucket BUCKET
#
## Example:
## s3-cp-latest-prefix.sh --bucket s3://abc

lorem='Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'

duis='Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Et harum quidem..'

## Command line options (and their respective defaults)
src="s3://abc"

## constants
sum1="xxhash2[ad97df912d23103f]"
sum2="xxhash2[ecb5ed42299ea74d]"

host="--host=s3.amazonaws.com"

## the metric that we closely check in this test
cold_counter="AWS-GET"

while (( "$#" )); do
  case "${1}" in
    --bucket) src=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

if ! [ -x "$(command -v s3cmd)" ]; then
  echo "Error: s3cmd not installed" >&2
  exit 1
fi
if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi

## uncomment for verbose output
set -x ## DEBUG

## establish existence
ais show bucket $src -c 1>/dev/null || exit $?

## temp destination bucket will be created on the fly
dst="ais://dst-$RANDOM"

## remember existing bucket's versioning; disable if need be
validate=$(ais bucket props show ${src} versioning.validate_warm_get -H | awk '{print $2}')
[[ "$validate" == "false"  ]] || ais bucket props set $src versioning.validate_warm_get=false
sync=$(ais bucket props show ${src} versioning.synchronize -H | awk '{print $2}')
[[ "$sync" == "false"  ]] || ais bucket props set $src versioning.synchronize=false

cleanup() {
  rc=$?
  ais object rm "$src/lorem-duis" 1>/dev/null 2>&1
  [[ "$validate" == "true"  ]] || ais bucket props set $src versioning.validate_warm_get=false 1>/dev/null 2>&1
  [[ "$sync" == "true"  ]] || ais bucket props set $src versioning.synchronize=false 1>/dev/null 2>&1
  ais rmb $dst --yes 1>/dev/null 2>&1
  exit $rc
}

trap cleanup EXIT INT TERM

echo -e
ais show performance counters --regex "(${cold_counter}$|VERSION-CHANGE$|DELETE)"
echo -e

echo "1. out-of-band PUT: 1st version"
echo $lorem | s3cmd put - "$src/lorem-duis" $host 1>/dev/null || exit $?

echo "2. copy, and check"
ais cp "$src/lorem-duis" $dst --all --wait  || exit $?
checksum=$(ais ls "$dst/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum1"  ]] || { echo "FAIL: $checksum != $sum1"; exit 1; }

echo "3. out-of-band PUT: 2nd version (overwrite)"
echo $duis | s3cmd put - "$src/lorem-duis" $host 1>/dev/null || exit $?

echo "4. copy and check (expecting the first version's checksum)"
ais cp "$src/lorem-duis" $dst --wait
checksum=$(ais ls "$dst/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" != "$sum2"  ]] || { echo "FAIL: $checksum == $sum2"; exit 1; }

echo "5. query cold-get count (statistics)"
cnt1=$(ais show performance counters --regex ${cold_counter} -H | awk '{sum+=$2;}END{print sum;}')

echo "6. copy latest: detect version change and update in-cluster copy"
ais cp "$src/lorem-duis" $dst --latest --wait
checksum=$(ais ls "$dst/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "7. cold-get counter must increment"
cnt2=$(ais show performance counters --regex ${cold_counter} -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt2 == $(($cnt1+1)) ]] || { echo "FAIL: $cnt2 != $(($cnt1+1))"; exit 1; }

echo "8. remember 'remote-deleted' counter"
cnt3=$(ais show performance counters --regex REMOTE-DEL -H | awk '{sum+=$2;}END{print sum;}')

echo "9. out-of-band DELETE"
s3cmd del "$src/lorem-duis" $host 1>/dev/null || exit $?

echo "10. copy '--latest' (expecting no changes)"
ais cp "$src/lorem-duis" $dst --latest --wait || exit $?
checksum=$(ais ls "$src/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

cnt4=$(ais show performance counters --regex REMOTE-DEL -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt4 == $cnt3 ]] || { echo "FAIL: $cnt4 != $cnt3"; exit 1; }

echo "11. run 'cp --sync' one last time, and make sure the object \"disappears\""
ais cp "$src/lorem-duis" --sync --wait

### [[ $? == 0 ]] || { echo "FAIL: expecting 'cp --wait' to return Ok, got $?"; exit 1; }

echo "12. 'remote-deleted' counter must increment"
cnt5=$(ais show performance counters --regex REMOTE-DEL -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt5 == $(($cnt3+1)) ]] || { echo "FAIL: $cnt5 != $(($cnt3+1))"; exit 1; }

ais ls "$src/lorem-duis" --cached --silent -H 2>/dev/null
[[ $? != 0 ]] || { echo "FAIL: expecting 'show object' error, got $?"; exit 1; }

echo -e
ais show performance counters --regex "(${cold_counter}$|VERSION-CHANGE$|DELETE)"
