#!/bin/bash

## Prerequisites: #################################################################################
# - aistore cluster with at least 3 active targets
#   (cleanup mode requires CountActiveTs >= 2; we take one target down, so need 3)
# - ais (CLI)
#
## What this tests:
# - 'ais start rebalance --cleanup' on a cluster where rebalance has left
#   misplaced copies behind (via maintenance + rejoin).
# - Asserts (after every rebalance phase): logical object count is preserved
#   end-to-end (no data loss); physical count grows after rejoin-rebalance
#   (misplaced copies exist) and returns to baseline after cleanup (copies
#   reclaimed); 'ais storage summary' agrees with 'ais ls --summary'.
#
## Usage:
## reb-cleanup-mode.sh [--bucket BUCKET] [--num-shards N]
#
## Example:
## ./ais/test/scripts/reb-cleanup-mode.sh
## ./ais/test/scripts/reb-cleanup-mode.sh --num-shards 9999

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi

## defaults
bucket="ais://reb-cleanup-$RANDOM"
num_shards=9999

while (( "$#" )); do
  case "${1}" in
    --bucket)     bucket=$2; shift; shift;;
    --num-shards) num_shards=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## uncomment for verbose output
## set -x

## state to be restored on exit
node=""
bucket_created=false

cleanup() {
  rc=$?
  ## bring node out of maintenance if we left it there mid-test
  [[ -z "$node" ]] || ais cluster add-remove-nodes stop-maintenance "$node" --yes 1>/dev/null 2>&1
  [[ "$bucket_created" == "false" ]] || ais rmb "$bucket" --yes 1>/dev/null 2>&1
  exit $rc
}
trap cleanup EXIT INT TERM

## --- measurement helpers (all use stable user-facing CLI surface) ---

## logical count: number of unique objects in BMD (data-integrity invariant)
get_logical() { ais ls "$bucket" --count-only | awk '{print $2}' | tr -d ','; }

## physical count: LOMs across all mountpaths, including misplaced copies
get_physical() { ais ls "$bucket" --summary -H | awk '{print $3}'; }

## storage summary: cached object count (cross-validates get_physical)
get_usage() { ais storage summary "$bucket" -H | awk '{print $2}'; }

assert_eq() {
  local label="$1" actual="$2" expected="$3"
  [[ "$actual" == "$expected" ]] || \
    { echo "FAIL: $label: got $actual, expected $expected"; exit 1; }
}

assert_gt() {
  local label="$1" actual="$2" floor="$3"
  [[ $actual -gt $floor ]] || \
    { echo "FAIL: $label: got $actual, expected > $floor"; exit 1; }
}

## 'ais wait rebalance' has a race window where it returns before a just-launched
## reb is registered as running. A second wait, after a brief pause, picks up
## the actual running xaction.
## TODO: fix 'ais wait rebalance' to wait for next state change (separate CLI work).
wait_reb() {
  ais wait rebalance
  sleep 1
  ais wait rebalance
}

## --- precheck: need >= 3 active targets ---
nat=$(ais show cluster target | awk '/^t\[/ && /online/' | wc -l)
[[ $nat -ge 3 ]] || { echo "FAIL: need >= 3 active targets, got $nat"; exit 1; }

## --- step 1: create bucket and fill with shards ---
ais create $bucket 1>/dev/null || exit $?
bucket_created=true
ais archive gen-shards "${bucket}/shard-{00001..$(printf %05d $num_shards)}.tar" \
    --fcount 1 --num-workers 10 1>/dev/null || exit $?

## --- step 2: baseline (logical and physical must agree initially) ---
logical_initial=$(get_logical)
physical_initial=$(get_physical)
usage_initial=$(get_usage)
assert_eq "initial logical"           "$logical_initial"  "$num_shards"
assert_eq "initial physical"          "$physical_initial" "$num_shards"
assert_eq "initial storage-summary"   "$usage_initial"    "$num_shards"

## --- step 3: pick a random target and put it in maintenance ---
node=$(ais advanced random-node)
ais cluster add-remove-nodes start-maintenance "$node" --yes 1>/dev/null || exit $?
wait_reb

## --- step 4: post-maintenance invariants ---
## logical preserved (rebalance redistributed; nothing lost)
## physical preserved (redistribution, not duplication)
assert_eq "post-maint logical"  "$(get_logical)"  "$logical_initial"
assert_eq "post-maint physical" "$(get_physical)" "$physical_initial"
assert_eq "post-maint usage"    "$(get_usage)"    "$usage_initial"

## --- step 5: bring node back; triggers rejoin-rebalance ---
ais cluster add-remove-nodes stop-maintenance "$node" --yes 1>/dev/null || exit $?
node=""  ## node is back; clear so trap doesn't try to stop-maintenance again
wait_reb

## --- step 6: post-rejoin invariants ---
## logical still preserved (full maintenance cycle without data loss)
## physical MUST exceed initial (rejoin-reb left misplaced copies behind —
## that's exactly what cleanup is for; if this assertion fails, the test is
## not exercising cleanup work and any subsequent "no-op cleanup" PASS would
## be meaningless)
assert_eq "post-rejoin logical"  "$(get_logical)" "$logical_initial"
physical_rejoin=$(get_physical)
usage_rejoin=$(get_usage)
assert_gt "post-rejoin physical" "$physical_rejoin" "$physical_initial"
assert_gt "post-rejoin usage"    "$usage_rejoin"    "$usage_initial"

## --- step 7: run cleanup ---
ais start rebalance --cleanup 1>/dev/null || exit $?
wait_reb

## --- step 8: post-cleanup invariants ---
## logical preserved (cleanup must never remove the last copy of an object)
## physical back to initial (all misplaced copies reclaimed)
## usage agrees with physical
assert_eq "post-cleanup logical"  "$(get_logical)"  "$logical_initial"
assert_eq "post-cleanup physical" "$(get_physical)" "$physical_initial"
assert_eq "post-cleanup usage"    "$(get_usage)"    "$usage_initial"

echo "PASS: cleanup reclaimed $((physical_rejoin - physical_initial)) misplaced copies"
