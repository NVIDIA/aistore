#!/bin/bash

## Prerequisites: #################################################################################
# - aistore cluster
# - remote aistore cluster (aka, "remais")
# - remais bucket
# - ais (CLI)
#
## Example usage:
## 1st, make sure remote ais cluster is attached, e.g.:
#  $ ais show remote-cluster -H
#  $ JcHy3JUrL  http://127.0.0.1:11080  remais    v9  1  11m22.312048996s
#
## 2nd, run:
#  $./ais/test/scripts/remais-get-validate.sh --bucket ais://@remais/abc     #####################

lorem='Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat.'

duis='Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. Et harum quidem..'

## Command line options (and their respective defaults)
bucket="s3://abc"

## constants
sum1="xxhash[ad97df912d23103f]"
sum2="xxhash[ecb5ed42299ea74d]"

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## uncomment for verbose output
## set -x

## must be @remais
[[ "$bucket" == *//@* ]] || { echo "Error: expecting remote ais bucket, got ${bucket}"; exit 1; }

## actual bucket inside remais:
rbucket="ais://$(basename ${bucket})"

## remais must be attached
rendpoint=$(ais show remote-cluster -H | awk '{print $2}')
[[ ! -z "$rendpoint" ]] || { echo "Error: no remote ais clusters"; exit 1; }

## check remote bucket; create if doesn't exist
exists=true
AIS_ENDPOINT=$rendpoint ais show bucket $rbucket -c 1>/dev/null 2>&1 || exists=false
[[ "$exists" == "true" ]] || AIS_ENDPOINT=$rendpoint ais create $rbucket || exit 1

## add it to _this_ cluster's BMD
ais show bucket $bucket --add 1>/dev/null || exit 1

## remember existing bucket 'validate_warm_get' setting; disable if need be
validate=$(ais bucket props show ${bucket} versioning.validate_warm_get -H | awk '{print $2}')
[[ "$validate" == "false"  ]] || ais bucket props set $bucket versioning.validate_warm_get=false
sync=$(ais bucket props show ${bucket} versioning.sync_warm_get -H | awk '{print $2}')
[[ "$sync" == "false"  ]] || ais bucket props set $bucket versioning.sync_warm_get=false

cleanup() {
  rc=$?
  ais object rm "$bucket/lorem-duis" 1>/dev/null 2>&1
  [[ "$validate" == "true"  ]] || ais bucket props set $bucket versioning.validate_warm_get=false 1>/dev/null 2>&1
  [[ "$sync" == "true"  ]] || ais bucket props set $bucket versioning.sync_warm_get=false 1>/dev/null 2>&1
  [[ "$exists" == "true" ]] || ais rmb $bucket -y 1>/dev/null 2>&1
  exit $rc
}

trap cleanup EXIT INT TERM

echo -e
ais show performance counters --regex "(GET-COLD$|VERSION-CHANGE$|DELETE)"
echo -e

echo "1. out-of-band PUT: first version"
echo $lorem | AIS_ENDPOINT=$rendpoint ais put - "$rbucket/lorem-duis" 1>/dev/null || exit $?

echo "2. cold GET, and check"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum1"  ]] || { echo "FAIL: $checksum != $sum1"; exit 1; }

echo "3. out-of-band PUT: 2nd version (overwrite)"
echo $duis | AIS_ENDPOINT=$rendpoint ais put - "$rbucket/lorem-duis" 1>/dev/null || exit $?

echo "4. warm GET and check (expecting the first version's checksum)"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" != "$sum2"  ]] || { echo "FAIL: $checksum == $sum2"; exit 1; }

echo "5. update bucket props: set validate-warm-get = true"
ais bucket props set $bucket versioning.validate_warm_get=true

echo "6. query cold-get count (statistics)"
cnt1=$(ais show performance counters --regex GET-COLD -H | awk '{sum+=$2;}END{print sum;}')

echo "7. warm GET: detect version change and trigger cold GET"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

echo "8. cold-get counter must increment"
cnt2=$(ais show performance counters --regex GET-COLD -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt2 == $(($cnt1+1)) ]] || { echo "FAIL: $cnt2 != $(($cnt1+1))"; exit 1; }

echo "9. 2nd warm GET must remain \"warm\" and cold-get-count must not increment"
ais get "$bucket/lorem-duis" /dev/null 1>/dev/null
checksum=$(ais ls "$bucket/lorem-duis" --cached -H -props checksum | awk '{print $2}')
[[ "$checksum" == "$sum2"  ]] || { echo "FAIL: $checksum != $sum2"; exit 1; }

cnt3=$(ais show performance counters --regex GET-COLD -H | awk '{sum+=$2;}END{print sum;}')
[[ $cnt3 == $cnt2 ]] || { echo "FAIL: $cnt3 != $cnt2"; exit 1; }

echo "10. out-of-band DELETE"
AIS_ENDPOINT=$rendpoint ais object rm "$rbucket/lorem-duis" 1>/dev/null || exit $?

echo "11. update bucket props: disable validate-warm-get _and_ enable sync-warm-get"
ais bucket props set $bucket versioning.validate_warm_get=false versioning.sync_warm_get=true

echo "12. warm GET must now trigger deletion"
ais get "$bucket/lorem-duis" /dev/null --silent 1>/dev/null 2>&1
[[ $? != 0 ]] || { echo "FAIL: expecting GET error, got $?"; exit 1; }
ais ls "$bucket/lorem-duis" --cached --silent -H 2>/dev/null
[[ $? != 0 ]] || { echo "FAIL: expecting 'show object' error, got $?"; exit 1; }

echo -e
ais show performance counters --regex "(GET-COLD$|VERSION-CHANGE$|DELETE)"
