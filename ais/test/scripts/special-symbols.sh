#!/bin/bash
#
## Prerequisites: ###############################################################################
# - running aistore cluster (AIS_ENDPOINT optional if non-default)
# - ais (CLI) built from the branch under test
#
## What this script tests:
# Verifies that object names containing URI-reserved characters are preserved only when
# '--encode-objname' is used, and demonstrates the artifacts created when it is not used.
#
## Example:
#  $ special-symbols-objname.sh
#  $ special-symbols-objname.sh --bucket ais://mytestbucket
#  $ special-symbols-objname.sh --keep
#################################################################################################

set -euo pipefail

if ! command -v ais >/dev/null 2>&1; then
  echo "Error: ais (CLI) not installed/in PATH" >&2
  exit 1
fi

# Defaults
bucket="ais://special-symbols-$RANDOM-$RANDOM"
keep=false

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift 2;;
    --keep) keep=true; shift;;
    *) echo "fatal: unknown argument '${1}'" >&2; exit 1;;
  esac
done

[[ "$bucket" == ais://* ]] || { echo "Error: expecting ais:// bucket, got: $bucket" >&2; exit 1; }
[[ "$bucket" != *"@remais"* ]] || { echo "Error: this script expects local AIS bucket (no @remais): $bucket" >&2; exit 1; }

FILE="$(mktemp)"

cleanup() {
  rm -f "$FILE"
  rc=$?
  if [[ "$keep" == "false" ]]; then
    ais rmb "$bucket" --yes >/dev/null 2>&1 || true
  fi
  exit $rc
}
trap cleanup EXIT

echo "hello world" > "$FILE"

echo "=== using bucket: $bucket ==="
ais create "$bucket" >/dev/null

# Stress names:
# 1) %2F decoding vs literal preservation
# 2) query/fragment truncation vs literal preservation
# 3) double-escape ladder
objs=(
  "a/b%2Fc/d"
  "x/y/z?u=v#frag"
  "p%252Fq"
)

needles=(
  "a/b%2Fc/d"
  "a/b/c/d"
  "x/y/z"
  "x/y/z?u=v#frag"
  "p%252Fq"
  "p%2Fq"
)

for obj in "${objs[@]}"; do
  echo
  echo "=============================="
  echo "OBJ NAME: $obj"
  echo "=============================="

  echo "-- put (no encoding)"
  ais put "$FILE" "$bucket/$obj" >/dev/null 2>&1 || true

  echo "-- put (with --encode-objname)"
  ais put "$FILE" "$bucket/$obj" --encode-objname >/dev/null

  echo "-- get (with --encode-objname) should succeed"
  ais get "$bucket/$obj" /dev/null --encode-objname >/dev/null
done

echo
echo "-- asserting expected artifacts exist (ls output contains expected names)"
out="$(ais ls "$bucket")"

for n in "${needles[@]}"; do
  echo "$out" | grep -Fq "$n" || { echo "FAIL: missing expected name in ls: $n" >&2; exit 1; }
done

echo "PASS"
