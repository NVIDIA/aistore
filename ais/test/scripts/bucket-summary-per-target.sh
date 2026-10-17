#!/bin/bash

## Prerequisites: AIStore with at least two active targets; ais and jq on PATH.
## Verify observed per-target counts and sizes, prefixes, aliases, JSON, and polling.
## Usage: bucket-summary-per-target.sh [--bucket ais://NEW_BUCKET]

set -euo pipefail

for tool in ais jq; do
  command -v "$tool" >/dev/null || { echo "Error: $tool is not installed" >&2; exit 1; }
done

bucket="ais://summary-per-target-$RANDOM-$$"
if [[ $# -gt 0 ]]; then
  [[ $# == 2 && $1 == --bucket && $2 == ais://* ]] || {
    echo "Usage: $0 [--bucket ais://NEW_BUCKET]" >&2
    exit 1
  }
  bucket=$2
fi

tmpdir=$(mktemp -d)
created=false
cleanup() {
  rc=$?
  trap - EXIT
  if [[ $created == true ]]; then
    ais rmb "$bucket" --yes >/dev/null 2>&1 || true
  fi
  rm -rf "$tmpdir"
  exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
# report the failing step (checks below are silent: jq -e, [[ ]])
trap 'echo "FAIL: line $LINENO: $BASH_COMMAND" >&2' ERR

step() { echo "-- $*"; }

# usage: expect_eq ACTUAL EXPECTED DESCRIPTION [FILE to dump on mismatch]
expect_eq() {
  [[ "$1" == "$2" ]] && return 0
  echo "FAIL: $3: got '$1', expected '$2'" >&2
  [[ -n "${4:-}" && -f "$4" ]] && sed 's/^/   /' "$4" >&2
  return 1
}

step "create $bucket"
# Never remove an existing bucket: cleanup is enabled only after creation succeeds.
ais create "$bucket" --props 'mirror.enabled=false ec.enabled=false' >/dev/null
created=true

check_json() {
  local expected=$1 file=$2
  jq -e --argjson expected "$expected" '
    length == 1 and
    (.[0] |
      .is_present and
      (.ObjCount.obj_count_present | tonumber) == $expected and
      (.per_target | length) >= 2 and
      ([.per_target[].is_present] | all) and
      ([.per_target[].ObjCount.obj_count_present | tonumber] | add) == $expected and
      ([.per_target[].TotalSize.size_all_present_objs | tonumber] | add) ==
        (.TotalSize.size_all_present_objs | tonumber))
  ' "$file" >/dev/null && return 0
  echo "FAIL: check_json: expected $expected objects in $(basename "$file"):" >&2
  jq -c '.[] | {name, is_present, objs: .ObjCount.obj_count_present, size: .TotalSize.size_all_present_objs,
    per_target: ((.per_target // {}) | map_values({objs: .ObjCount.obj_count_present, is_present}))}' "$file" >&2 ||
    sed 's/^/   /' "$file" >&2
  return 1
}

step "empty bucket: JSON"
ais storage summary "$bucket" --per-target --json > "$tmpdir/empty.json"
check_json 0 "$tmpdir/empty.json"
ntargets=$(jq '.[0].per_target | length' "$tmpdir/empty.json")
echo "   targets: $ntargets"
step "empty bucket: provider query (ais:)"
# A query matching just one bucket must retain that bucket's identity.
ais storage summary ais: --per-target --json |
  jq --arg name "${bucket#ais://}" '[.[] | select(.name == $name)]' > "$tmpdir/query.json"
check_json 0 "$tmpdir/query.json"

step "generate 30 + 60 shards"
ais archive gen-shards "$bucket/images/shard-{001..030}.tar" --fcount 1 --fsize 1KiB --num-workers 4 >/dev/null
ais archive gen-shards "$bucket/other/shard-{001..060}.tar" --fcount 1 --fsize 4KiB --num-workers 4 >/dev/null

step "bucket, prefix, and query summaries: JSON"
ais storage summary "$bucket" --per-target --json > "$tmpdir/all.json"
check_json 90 "$tmpdir/all.json"
ais bucket summary "$bucket/images/" --per-target --json > "$tmpdir/prefix.json"
check_json 30 "$tmpdir/prefix.json"
ais storage summary --prefix images/ --per-target --json |
  jq --arg name "${bucket#ais://}" '[.[] | select(.name == $name)]' > "$tmpdir/query.json"
check_json 30 "$tmpdir/query.json"

# -H contains only target rows, with the same counts and object bytes as JSON.
step "per-target rows (-H): counts, bytes, shares"
ais storage summary "$bucket" --per-target -H --units raw > "$tmpdir/table"
expect_eq "$(wc -l < "$tmpdir/table")" "$ntargets" "-H row count" "$tmpdir/table"
expect_eq "$(awk '{sum += $3} END {print sum}' "$tmpdir/table")" 90 "-H object count sum" "$tmpdir/table"
bytes=$(jq -r '.[0].TotalSize.size_all_present_objs' "$tmpdir/all.json")
expect_eq "$(awk '{sum += $5} END {printf "%.0f", sum}' "$tmpdir/table")" "$bytes" "-H object bytes sum" "$tmpdir/table"
if ! awk -v bytes="$bytes" '
  function abs(n) { return n < 0 ? -n : n }
  abs($4 - 100 * $3 / 90) > 0.051 || abs($6 - 100 * $5 / bytes) > 0.051 { exit 1 }
' "$tmpdir/table"; then
  echo "FAIL: -H shares do not match counts/bytes:" >&2; sed 's/^/   /' "$tmpdir/table" >&2; exit 1
fi
step "ais ls --summary --per-target -H"
ais ls "$bucket/images/" --summary --per-target -H --units raw > "$tmpdir/list"
expect_eq "$(wc -l < "$tmpdir/list")" "$ntargets" "ls -H row count" "$tmpdir/list"
expect_eq "$(awk '{sum += $3} END {print sum}' "$tmpdir/list")" 30 "ls -H object count sum" "$tmpdir/list"
step "table headers"
ais storage summary "$bucket" --per-target > "$tmpdir/headers"
grep -q 'OBJECT SHARE(%)' "$tmpdir/headers"
grep -q 'SIZE SHARE(%)' "$tmpdir/headers"
grep -Eq 'OBJECT SHARE\(%\)[[:space:]]+SIZE[[:space:]]+SIZE SHARE\(%\)' "$tmpdir/headers"
step "--refresh: -H and JSON"
ais storage summary "$bucket" --per-target -H --refresh 1s --count 1 > "$tmpdir/table"
expect_eq "$(wc -l < "$tmpdir/table")" "$ntargets" "-H --refresh row count" "$tmpdir/table"
ais storage summary "$bucket" --per-target --json --refresh 1s --count 2 |
  jq -se 'length == 2 and all(.[]; length == 1 and (.[0].ObjCount.obj_count_present | tonumber) == 90)' >/dev/null

step "--dont-wait: poll by job ID"
# Pending JSON is a job ID; polling eventually returns the summary array.
xid=$(ais storage summary "$bucket" --prefix images/ --per-target --dont-wait --json | jq -er 'select(type == "string" and length > 0)')
for ((attempt=0; attempt<50; attempt++)); do
  ais storage summary "$bucket" "$xid" --prefix images/ --per-target --dont-wait --json > "$tmpdir/poll.json" 2> "$tmpdir/poll.err"
  if jq -e 'type == "array"' "$tmpdir/poll.json" >/dev/null && ! grep -q 'still running' "$tmpdir/poll.err"; then
    check_json 30 "$tmpdir/poll.json"
    break
  fi
  jq -e --arg xid "$xid" 'type == "array" or . == $xid' "$tmpdir/poll.json" >/dev/null
  sleep 0.2
done
expect_eq "$((attempt < 50))" 1 "polling attempts ($attempt)" "$tmpdir/poll.err"

step "invalid requests"
# Invalid requests fail locally instead of starting another summary job.
if ais ls "$bucket" --per-target > "$tmpdir/error" 2>&1; then
  echo "FAIL: --per-target without --summary succeeded" >&2; exit 1
fi
grep -q 'requires.*summary' "$tmpdir/error"
if ais storage summary "$bucket" bad.id --dont-wait > "$tmpdir/error" 2>&1; then
  echo "FAIL: invalid job ID succeeded" >&2; exit 1
fi
grep -q 'invalid job ID' "$tmpdir/error"
if ais storage summary "$bucket" "$xid" > "$tmpdir/error" 2>&1; then
  echo "FAIL: polling without --dont-wait succeeded" >&2; exit 1
fi
grep -q 'JOB_ID requires' "$tmpdir/error"
if ais storage summary "ais://per_target-missing-$RANDOM-$$" --per-target > "$tmpdir/error" 2>&1; then
  echo "FAIL: missing bucket summary succeeded" >&2; exit 1
fi
if grep -q 'requires v5.2 or later' "$tmpdir/error"; then
  echo "FAIL: missing bucket was reported as an old proxy" >&2; exit 1
fi

echo "PASS: per-target bucket summary across $ntargets targets (90 objects); prefixes, tables, JSON, and polling"
