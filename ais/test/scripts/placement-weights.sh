#!/bin/bash

## Prerequisites: AIStore with at least three active targets, no placement weights, rebalance enabled; ais and jq on PATH.
## Verify target placement weights with data:
## - per-target object counts (measured on targets) follow the weights 1:2:3
## - no misplaced objects after rebalance and cleanup
## - proportional weights move nothing; clearing weights restores the original placement
## Usage: placement-weights.sh [--bucket ais://NEW_BUCKET] [--num NUM_OBJECTS]

set -euo pipefail

for tool in ais jq; do
  command -v "$tool" >/dev/null || { echo "Error: $tool is not installed" >&2; exit 1; }
done

bucket="ais://placement-weights-$RANDOM-$$"
num=3000
while (( $# )); do
  case "$1" in
    --bucket) [[ ${2:-} == ais://* ]] || { echo "Usage: $0 [--bucket ais://NEW_BUCKET] [--num NUM_OBJECTS]" >&2; exit 1; }
              bucket=$2; shift 2;;
    --num)    num=$2; shift 2;;
    *)        echo "Usage: $0 [--bucket ais://NEW_BUCKET] [--num NUM_OBJECTS]" >&2; exit 1;;
  esac
done

tmpdir=$(mktemp -d)
created=false
weighted=false
cleanup() {
  rc=$?
  trap - EXIT
  if [[ $created == true ]]; then
    ais rmb "$bucket" --yes >/dev/null 2>&1 || true
  fi
  if [[ $weighted == true ]]; then
    echo "-- cleanup: clear weights"
    ais cluster placement clear-weights >/dev/null 2>&1 && ais wait rebalance >/dev/null 2>&1 || true
  fi
  rm -rf "$tmpdir"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'echo "FAIL: line $LINENO: $BASH_COMMAND" >&2' ERR

step() { echo "-- $*"; }
fail() { echo "FAIL: $*" >&2; exit 1; }

# per-target object counts ("ID COUNT" lines, sorted by ID) => $1
counts() {
  ais storage summary "$bucket" --per-target --json |
    jq -r '.[0].per_target | to_entries[] | "\(.key) \(.value.ObjCount.obj_count_present)"' | sort > "$1"
}

# wait for rebalance, then remove misplaced copies until per-target counts add up to $num
settle() {
  ais wait rebalance >/dev/null
  local attempt total
  for ((attempt=1; attempt<=6; attempt++)); do
    counts "$tmpdir/counts"
    total=$(awk '{sum += $2} END {print sum+0}' "$tmpdir/counts")
    [[ $total == "$num" ]] && return 0
    echo "   $((total - num)) misplaced copies: cleanup pass $attempt"
    ais job start rebalance --cleanup >/dev/null
    ais wait rebalance >/dev/null
  done
  fail "per-target counts add up to $total, expected $num, after $((attempt - 1)) cleanup passes"
}

# 'ais storage validate': no misplaced objects
check_misplaced() {
  local misplaced
  misplaced=$(ais storage validate "$bucket" --all-columns -H | awk -F'\t+' 'NR == 1 {gsub(/ /, "", $3); print $3}')
  [[ $misplaced == "-" || $misplaced == "0" ]] || fail "$bucket: $misplaced misplaced objects"
  echo "   misplaced: none"
}

# usage: check_shares WEIGHT_1 WEIGHT_2 ... (in target ID order)
# per-target counts must be within a generous statistical bound of num * weight / sum(weights)
check_shares() {
  counts "$tmpdir/counts"
  local -a weights=("$@")
  awk -v n="$num" -v ws="${weights[*]}" '
    BEGIN { k = split(ws, w, " "); for (i = 1; i <= k; i++) sum += w[i] }
    {
      p = w[NR] / sum; want = n * p; limit = 6 * sqrt(n * p * (1 - p)) + 2
      d = $2 - want; if (d < 0) d = -d
      printf "   %s  weight %s  objects %5d  expected %7.1f +/- %.1f\n", $1, w[NR], $2, want, limit
      if (d > limit) bad = 1
    }
    END { if (NR != k) { print "   expected " k " targets, got " NR; exit 1 } ; exit bad }
  ' "$tmpdir/counts" || fail "per-target counts do not follow weights (${weights[*]})"
}

step "cluster"
ais show cluster smap --json | jq -r '.tmap | to_entries[] | select((.value.flags // 0) == 0) | .key' | sort > "$tmpdir/tids"
mapfile -t tids < "$tmpdir/tids"
(( ${#tids[@]} >= 3 )) || fail "need at least 3 active targets, have ${#tids[@]}"
ais cluster placement | grep -q 'uniform' || fail "cluster already has placement weights"
echo "   active targets: ${tids[*]}"

step "create $bucket and generate $num objects"
ais create "$bucket" --props 'mirror.enabled=false ec.enabled=false' >/dev/null
created=true
ais archive gen-shards "$bucket/obj-{1..$num}.tar" --fcount 1 --fsize 1KiB --num-workers 8 >/dev/null

step "no weights: uniform"
settle
uniform=(); for _ in "${tids[@]}"; do uniform+=(1); done
check_shares "${uniform[@]}"
cp "$tmpdir/counts" "$tmpdir/baseline"
check_misplaced

step "set weights 1:2:3 (any other targets: 1)"
args=("${tids[0]}=1" "${tids[1]}=2" "${tids[2]}=3" rest=1)
weighted=true
ais cluster placement set-weights "${args[@]}"
settle
ws=(1 2 3); for ((i=3; i<${#tids[@]}; i++)); do ws+=(1); done
check_shares "${ws[@]}"
cp "$tmpdir/counts" "$tmpdir/weighted"
check_misplaced
ais cluster placement

step "proportional weights (x1000): no rebalance, nothing moves"
out=$(ais cluster placement set-weights "${tids[0]}=1000" "${tids[1]}=2000" "${tids[2]}=3000" rest=1000)
echo "   $out"
[[ $out != *"Started rebalance"* ]] || fail "proportional weights started rebalance"
counts "$tmpdir/counts"
cmp -s "$tmpdir/counts" "$tmpdir/weighted" || fail "proportional weights changed per-target counts"

step "clear weights: original placement"
ais cluster placement clear-weights
weighted=false
settle
cmp -s "$tmpdir/counts" "$tmpdir/baseline" || { diff "$tmpdir/baseline" "$tmpdir/counts" >&2 || true; fail "clearing weights did not restore the original placement"; }
check_misplaced

echo "PASS"
