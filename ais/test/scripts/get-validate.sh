#!/bin/bash

## Example usage:
## ./ais/test/scripts/get-validate.sh --bucket s3://abc

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
    --nocleanup) nocleanup="true"; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## set -x # uncomment for verbose output

## establish existence
ais show bucket $bucket -c 1>/dev/null || exit $?

cleanup() {
  rc=$?
  ais object rm "$bucket/lorem-duis" 1>/dev/null 2>&1
  ais bucket evict $bucket 1>/dev/null 2>&1
  exit $rc
}

trap cleanup EXIT INT TERM

echo "out-of-band PUT first version"
echo $lorem | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "cold GET and check"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum1"  ]] || { echo "FAIL: $checksum != $sum1"; exit 1; }

echo "out-of-band PUT 2nd version (overwrite)"
echo $duis | s3cmd put - "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "warm GET and check (expecting the first version's checksum)"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" != "$sum2"  ]] || { echo "FAIL: $checksum == $sum2"; exit 1; }

echo "update bucket props: set validate-warm-get = true"
ais bucket props set $bucket versioning.validate_warm_get=true

echo "query cold-get-count (statistics)"
cnt1=$(ais show performance counters --all -H | awk '{sum+=$3;}END{print sum;}')

echo "warm GET must trigger cold GET"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "cold-get-count must increment"
cnt2=$(ais show performance counters --all -H | awk '{sum+=$3;}END{print sum;}')
[[ $cnt2 == $(($cnt1+1)) ]] || { echo "FAIL: $cnt2 != $(($cnt1+1))"; exit 1; }

echo "2nd warm GET must remain \"warm\" and cold-get-count must _not_ increment"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

cnt3=$(ais show performance counters --all -H | awk '{sum+=$3;}END{print sum;}')
[[ $cnt3 == $cnt2 ]] || { echo "FAIL: $cnt3 != $cnt2"; exit 1; }

echo "DELETE out-of-band"
s3cmd del "$bucket/lorem-duis" $host 1>/dev/null || exit $?

echo "warm GET must (silently) trigger deletion"
ais get "$bucket/lorem-duis" /dev/null --silent 1>/dev/null 2>&1
[[ $? != 0 ]] || { echo "FAIL: expecting GET error, got $?"; exit 1; }
ais ls "$bucket/lorem-duis" --cached --silent -H 2>/dev/null
[[ $? != 0 ]] || { echo "FAIL: expecting 'show object' error, got $?"; exit 1; }

echo Done.
