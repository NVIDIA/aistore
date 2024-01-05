#!/bin/bash

## Prerequisites: #################################################################################
# - s3 bucket
# - s3cmd, $PATH-executable and configured to access the bucket out-of-band
# - aistore cluster, also configured to access the same bucket
#
## Example usage:
## ./ais/test/scripts/s3-prefetch-latest.sh --bucket s3://abc              ########################

lorem='Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'

duis='Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Et harum quidem..'

## Command line options (and their respective defaults)
bucket="s3://abc"

## constants
sum1="xxhash[ad97df912d23103f]"
sum2="xxhash[ecb5ed42299ea74d]"

host="--host=s3.amazonaws.com"

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## uncomment for verbose output
## set -x

## establish existence
ais show bucket $bucket -c 1>/dev/null || exit $?

## remember existing bucket's versioning; disable if need be
sync=$(ais bucket props show ${bucket} versioning.sync_warm_get -H | awk '{print $2}')
[[ "$sync" == "false"  ]] || ais bucket props set $bucket versioning.sync_warm_get=false

cleanup() {
  rc=$?
  ais object rm "$bucket/lorem-duis" 1>/dev/null 2>&1
  [[ "$sync" == "true"  ]] || ais bucket props set $bucket versioning.sync_warm_get=false 1>/dev/null 2>&1
  exit $rc
}

trap cleanup EXIT INT TERM

echo -e
ais show performance counters --regex "(GET-COLD$|VERSION-CHANGE$|DELETE)"
echo -e

echo "1. out-of-band PUT: first version"
echo $lorem | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "2. prefetch, and check"
ais prefetch "$bucket/lorem-duis" --wait
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum1"  ]] || { echo "FAIL: $checksum != $sum1"; exit 1; }

echo "3. out-of-band PUT: 2nd version (overwrite)"
echo $duis | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "4. prefetch and check (expecting the first version's checksum)"
ais prefetch "$bucket/lorem-duis" --wait
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" != "$sum2"  ]] || { echo "FAIL: $checksum == $sum2"; exit 1; }

echo "5. query cold-get count (statistics)"
cnt1=$(ais show performance counters --regex GET-COLD -H | awk '{sum+=$2;}END{print sum;}')

echo "6. prefetch latest: detect version change and update in-cluster copy"
ais prefetch "$bucket/lorem-duis" --latest --wait
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "7. cold-get counter must increment"
cnt2=$(ais show performance counters --regex GET-COLD -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt2 == $(($cnt1+1)) ]] || { echo "FAIL: $cnt2 != $(($cnt1+1))"; exit 1; }

echo "8. warm GET must remain \"warm\" and cold-get-count must not increment"
ais get "$bucket/lorem-duis" /dev/null --latest 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

cnt3=$(ais show performance counters --regex GET-COLD -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt3 == $cnt2 ]] || { echo "FAIL: $cnt3 != $cnt2"; exit 1; }

echo "9. out-of-band DELETE"
s3cmd del "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "10. prefetch with no '--latest' switch should still be fine"
ais prefetch "$bucket/lorem-duis" --wait
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "11. remember 'remote-deleted' counter and update bucket props: set sync-warm-get = true"
cnt4=$(ais show performance counters --regex REMOTE-DEL -H | awk '{sum+=$2;}END{print sum;}')
ais bucket props set $bucket versioning.sync_warm_get=true

echo "12. run 'prefetch --latest' one last time, and make sure the object \"disappears\""
ais prefetch "$bucket/lorem-duis" --latest --wait 2>/dev/null
[[ $? == 0 ]] || { echo "FAIL: expecting 'prefetch --wait' to return Ok, got $?"; exit 1; }

echo "13. 'remote-deleted' counter must increment"
cnt5=$(ais show performance counters --regex REMOTE-DEL -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt5 == $(($cnt4+1)) ]] || { echo "FAIL: $cnt5 != $(($cnt4+1))"; exit 1; }


ais ls "$bucket/lorem-duis" --cached --silent -H 2>/dev/null
[[ $? != 0 ]] || { echo "FAIL: expecting 'show object' error, got $?"; exit 1; }

echo -e
ais show performance counters --regex "(GET-COLD$|VERSION-CHANGE$|DELETE)"
