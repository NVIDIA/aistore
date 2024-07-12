#!/bin/bash

## Prerequisites: #################################################################################
# - s3 bucket
# - s3cmd, $PATH-executable and configured to access the bucket out-of-band
# - aistore cluster, also configured to access the same bucket
#
## Usage:
## s3-get-validate.sh --bucket BUCKET
#
## Example:
## s3-get-validate.sh --bucket s3://abc

lorem='Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'

duis='Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Et harum quidem..'

## Command line options (and their respective defaults)
bucket="s3://abc"

## constants
sum1="xxhash[ad97df912d23103f]"
sum2="xxhash[ecb5ed42299ea74d]"

host="--host=s3.amazonaws.com"

## the metric that we closely check in this test
cold_counter="AWS-GET"

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
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
## set -x

## establish existence
ais show bucket $bucket -c 1>/dev/null || exit $?

## remember existing bucket's versioning; disable if need be
validate=$(ais bucket props show ${bucket} versioning.validate_warm_get -H | awk '{print $2}')
[[ "$validate" == "false"  ]] || ais bucket props set $bucket versioning.validate_warm_get=false
sync=$(ais bucket props show ${bucket} versioning.synchronize -H | awk '{print $2}')
[[ "$sync" == "false"  ]] || ais bucket props set $bucket versioning.synchronize=false

cleanup() {
  rc=$?
  ais object rm "$bucket/lorem-duis" 1>/dev/null 2>&1
  [[ "$validate" == "true"  ]] || ais bucket props set $bucket versioning.validate_warm_get=false 1>/dev/null 2>&1
  [[ "$sync" == "true"  ]] || ais bucket props set $bucket versioning.synchronize=false 1>/dev/null 2>&1
  exit $rc
}

trap cleanup EXIT INT TERM

echo -e
ais show performance counters --regex "(${cold_counter}$|VERSION-CHANGE$|DELETE)"
echo -e

echo "1. out-of-band PUT: first version"
echo $lorem | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "2. cold GET, and check"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum1"  ]] || { echo "FAIL: $checksum != $sum1"; exit 1; }

echo "3. out-of-band PUT: 2nd version (overwrite)"
echo $duis | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

######### --latest

echo "3.1 'get --latest' without changing bucket props"

ais get "$bucket/lorem-duis" /dev/null --latest 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "3.2 restore the state prior to step 3.1"

echo $lorem | ais put - "$bucket/lorem-duis" 1>/dev/null || exit $?
echo $duis  | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

######### end of --latest

echo "4. warm GET and check (expecting the first version's checksum)"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" != "$sum2"  ]] || { echo "FAIL: $checksum == $sum2"; exit 1; }

echo "5. update bucket props: set validate-warm-get = true"
ais bucket props set $bucket versioning.validate_warm_get=true

echo "6. query cold-get count (statistics)"
cnt1=$(ais show performance counters --regex ${cold_counter} -H | awk '{sum+=$2;}END{print sum;}')

echo "7. warm GET: detect version change and perform cold GET"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "8. cold-get counter must increment"
cnt2=$(ais show performance counters --regex ${cold_counter} -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt2 == $(($cnt1+1)) ]] || { echo "FAIL: $cnt2 != $(($cnt1+1))"; exit 1; }

echo "9. 2nd warm GET must remain \"warm\" and cold-get-count must not increment"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

cnt3=$(ais show performance counters --regex ${cold_counter} -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt3 == $cnt2 ]] || { echo "FAIL: $cnt3 != $cnt2"; exit 1; }

echo "10. out-of-band DELETE"
s3cmd del "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "10.1. update bucket props: disable validate-warm-get"
ais bucket props set $bucket versioning.validate_warm_get=false

echo "11. warm GET must be fine"
ais get "$bucket/lorem-duis" /dev/null --silent 1>/dev/null 2>&1
[[ $? == 0 ]] || { echo "FAIL: expecting warm GET to succeed, got $?"; exit 1; }

echo "12. remember 'remote-deleted' counter and enable version synchronization"
ais bucket props set $bucket versioning.synchronize=true

cnt4=$(ais show performance counters --regex DELETED -H | awk '{sum+=$2;}END{print sum;}')

echo "13. this time, warm GET must trigger deletion and return 'not found', 'remote-deleted' must increment"
ais get "$bucket/lorem-duis" /dev/null --silent 1>/dev/null 2>&1
[[ $? != 0 ]] || { echo "FAIL: expecting GET error, got $?"; exit 1; }
ais ls "$bucket/lorem-duis" --cached --silent -H 2>/dev/null
[[ $? != 0 ]] || { echo "FAIL: expecting 'show object' error, got $?"; exit 1; }

cnt5=$(ais show performance counters --regex DELETED -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt5 == $(($cnt4+1)) ]] || { echo "FAIL: $cnt5 != $(($cnt4+1))"; exit 1; }

echo -e
ais show performance counters --regex "(${cold_counter}$|VERSION-CHANGE$|DELETE)"
