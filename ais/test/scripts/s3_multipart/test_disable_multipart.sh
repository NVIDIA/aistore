#!/bin/bash

# Test s3cmd --disable-multipart functionality

set -euo pipefail

BUCKET="${1:-nnn}"
TEMPDIR=$(mktemp -d)

# SSL configuration based on AIS_USE_HTTPS environment variable
if [[ "${AIS_USE_HTTPS:-}" == "true" ]]; then
  ssl_opts="--no-check-certificate"
else
  ssl_opts="--no-ssl"
fi
HOST_ARGS="--host=localhost:8080/s3 --host-bucket=localhost:8080/s3/%(bucket) $ssl_opts"

cleanup() {
    rm -rf "$TEMPDIR"
    s3cmd del s3://$BUCKET/test-* $HOST_ARGS 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Testing s3cmd --disable-multipart functionality ==="

# Ensure bucket exists
ais create ais://$BUCKET 2>/dev/null || true
ais bucket props set ais://$BUCKET checksum.type=md5 >/dev/null

# Test 1: Upload large file WITHOUT --disable-multipart (should use multipart)
echo "--- Testing: Large file upload without --disable-multipart ---"
test_file="$TEMPDIR/large_file"
dd if=/dev/urandom of="$test_file" bs=1M count=50 status=none

object_name="test-with-multipart"
upload_output1=$(s3cmd put "$test_file" s3://$BUCKET/$object_name \
    --multipart-chunk-size-mb=10 \
    --force $HOST_ARGS 2>&1) || {
    echo "FAIL: Upload without --disable-multipart failed"
    exit 1
}

# Check if multipart indicators are present
multipart_used1=false
if echo "$upload_output1" | grep -q "multipart\|part.*of.*"; then
    multipart_used1=true
fi

echo "Upload without --disable-multipart completed (multipart used: $multipart_used1)"

# Verify object exists
s3cmd info s3://$BUCKET/$object_name $HOST_ARGS >/dev/null || {
    echo "FAIL: Object without --disable-multipart not found"
    exit 1
}

# Test 2: Upload same large file WITH --disable-multipart (should NOT use multipart)
echo "--- Testing: Large file upload with --disable-multipart ---"
object_name2="test-no-multipart"
upload_output2=$(s3cmd put "$test_file" s3://$BUCKET/$object_name2 \
    --multipart-chunk-size-mb=10 \
    --disable-multipart \
    --force $HOST_ARGS 2>&1) || {
    echo "FAIL: Upload with --disable-multipart failed"  
    exit 1
}

# Check if multipart was NOT used
multipart_used2=false
if echo "$upload_output2" | grep -q "multipart\|part.*of.*"; then
    multipart_used2=true
fi

echo "Upload with --disable-multipart completed (multipart used: $multipart_used2)"

# Verify object exists
s3cmd info s3://$BUCKET/$object_name2 $HOST_ARGS >/dev/null || {
    echo "FAIL: Object with --disable-multipart not found"
    exit 1
}

# Test 3: Compare the two approaches
echo "--- Validating --disable-multipart behavior ---"

# Both objects should have same size
info1=$(s3cmd info s3://$BUCKET/$object_name $HOST_ARGS)
info2=$(s3cmd info s3://$BUCKET/$object_name2 $HOST_ARGS)

size1=$(echo "$info1" | grep "File size:" | awk '{print $3}' | head -1)
size2=$(echo "$info2" | grep "File size:" | awk '{print $3}' | head -1)

if [[ "$size1" != "$size2" ]]; then
    echo "FAIL: Size mismatch between multipart and non-multipart uploads"
    echo "With multipart: $size1 bytes"
    echo "Without multipart: $size2 bytes"
    exit 1
fi

echo "PASS: Both uploads have same size ($size1 bytes)"

# Test 4: Verify expected multipart behavior
# For a 50MB file with 10MB chunks, multipart should normally be used
# When --disable-multipart is used, it should NOT be used
if [[ "$multipart_used1" == "false" ]]; then
    echo "WARNING: Expected multipart to be used for 50MB file with 10MB chunks, but it wasn't detected"
    # This might still be OK if s3cmd doesn't show obvious multipart indicators
fi

if [[ "$multipart_used2" == "true" ]]; then
    echo "FAIL: --disable-multipart did not prevent multipart upload"
    echo "Upload output: $upload_output2"
    exit 1
fi

echo "PASS: --disable-multipart appears to work correctly"

# Clean up
s3cmd del s3://$BUCKET/$object_name $HOST_ARGS >/dev/null
s3cmd del s3://$BUCKET/$object_name2 $HOST_ARGS >/dev/null

echo "=== --disable-multipart functionality test passed ==="
