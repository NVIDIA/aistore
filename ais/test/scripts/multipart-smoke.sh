#!/bin/bash

## Prerequisites: #################################################################################
# - s3cmd, $PATH-executable and configured to access S3
# - aistore cluster, configured and running
# - xxhsum utility for checksum verification
#
## Usage:
## multipart-smoke-test.sh --bucket BUCKET [--filename FILE] [--size-mb SIZE]
#
## Examples:
## multipart-smoke-test.sh --bucket s3://my-s3-bucket
## multipart-smoke-test.sh --bucket ais://my-ais-bucket --size-mb 256
## multipart-smoke-test.sh --bucket ais://test-bucket --filename testfile.bin --size-mb 64

set -euo pipefail

## Command line options (and their respective defaults)
bucket=""
filename="largefile"
size_mb=128

## Parse command line arguments
while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    --filename) filename=$2; shift; shift;;
    --size-mb) size_mb=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## Validate required arguments
if [[ -z "$bucket" ]]; then
  echo "Error: --bucket is required"
  echo "Usage: $0 --bucket BUCKET [--filename FILE] [--size-mb SIZE]"
  exit 1
fi

## Dependency checks
if ! [ -x "$(command -v s3cmd)" ]; then
  echo "Error: s3cmd not installed" >&2
  exit 1
fi
if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi
if ! [ -x "$(command -v xxhsum)" ]; then
  echo "Error: xxhsum not installed" >&2
  exit 1
fi

## File paths
src="/tmp/${filename}"
dst="/tmp/${filename}.get-result"

## Determine bucket type and setup
bucket_created=false
if [[ "$bucket" == ais://* ]]; then
  bucket_type="ais"
  bucket_name="${bucket#ais://}"
  s3_bucket="s3://${bucket_name}"

  # Check if AIS bucket exists, create if not
  if ! ais show bucket "$bucket" -c >/dev/null 2>&1; then
    echo ">> creating AIS bucket: $bucket"
    ais create "$bucket" || exit $?
    bucket_created=true
  fi

  # Check/set checksum type for AIS bucket
  current_checksum=$(ais bucket props show "$bucket" checksum.type -H | awk '{print $2}')
  if [[ "$current_checksum" != "md5" ]]; then
    if [[ "$bucket_created" == "true" ]]; then
      # New bucket - set to md5
      ais bucket props set "$bucket" checksum.type md5 >/dev/null || exit $?
    else
      echo "Error: existing bucket $bucket has checksum type '$current_checksum', expected 'md5'"
      exit 1
    fi
  fi

elif [[ "$bucket" == s3://* ]]; then
  bucket_type="s3"
  bucket_name="${bucket#s3://}"
  s3_bucket="$bucket"

  # Verify bucket exists
  if ! ais show bucket "$bucket" -c >/dev/null 2>&1; then
    echo "Error: $bucket does not exist"
    exit 1
  fi

else
  echo "Error: bucket must start with 's3://' or 'ais://'"
  exit 1
fi

## SSL configuration based on AIS_USE_HTTPS environment variable
if [[ "${AIS_USE_HTTPS:-}" == "true" ]]; then
  # HTTPS mode: use SSL, skip cert verification for self-signed certs
  ssl_opts="--no-check-certificate"
else
  # HTTP mode: disable SSL
  ssl_opts="--no-ssl"
fi

## uncomment for verbose output
# set -x ## DEBUG

cleanup() {
  rc=$?
  rm -f "$src" "$dst" 2>/dev/null

  # Remove AIS bucket if we created it
  if [[ "$bucket_created" == "true" ]]; then
    echo ">> cleaning up: removing created AIS bucket $bucket"
    ais rmb "$bucket" --yes >/dev/null 2>&1
  fi

  exit $rc
}

trap cleanup EXIT INT TERM

echo ">> Bucket type: $bucket_type"
echo ">> Target bucket: $bucket"
echo ">> S3 endpoint: $s3_bucket"
echo -e

echo "1. generating ${size_mb} MiB random file at ${src}..."
dd if=/dev/urandom of="$src" bs=1M count="$size_mb" status=progress

echo "2. computing xxhash of source file..."
h1=$(xxhsum "$src" | awk '{print $1}')
echo "   Source hash: $h1"

echo "3. uploading via s3cmd to $s3_bucket..."
s3cmd put "$src" "$s3_bucket/${filename}" \
  $ssl_opts \
  --host=localhost:8080/s3 \
  --host-bucket="localhost:8080/s3/%(bucket)" \
|| exit $?

echo "4. downloading via s3cmd from $s3_bucket..."
s3cmd get "$s3_bucket/${filename}" "$dst" --force \
  $ssl_opts \
  --host=localhost:8080/s3 \
  --host-bucket="localhost:8080/s3/%(bucket)" \
|| exit $?

echo "5. computing xxhash of downloaded file..."
h2=$(xxhsum "$dst" | awk '{print $1}')
echo "   Downloaded hash: $h2"

echo "6. verifying checksums..."
if [[ "$h1" == "$h2" ]]; then
  echo "PASS: checksums match ($h1)"

  # Additional verification for AIS buckets
  if [[ "$bucket_type" == "ais" ]]; then
    echo "7. verifying AIS bucket contains the object..."
    if ais ls "$bucket/$filename" --silent >/dev/null 2>&1; then
      echo "PASS: object found in AIS bucket"
    else
      echo "FAIL: object not found in AIS bucket"
      exit 1
    fi
  fi

  exit 0
else
  echo "FAIL: checksum mismatch - $h1 != $h2"
  exit 1
fi
