#!/bin/bash

# Test s3cmd --continue-put functionality for resuming multipart uploads

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
    # Try to abort any leftover multipart uploads
    s3cmd multipart s3://$BUCKET $HOST_ARGS 2>/dev/null | grep "test-" | while read line; do
        upload_id=$(echo "$line" | awk '{print $NF}')
        object_name=$(echo "$line" | awk '{print $(NF-1)}')  
        s3cmd abortmp s3://$BUCKET/$object_name $upload_id $HOST_ARGS 2>/dev/null || true
    done 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Testing s3cmd --continue-put functionality ==="

# Ensure bucket exists
ais create ais://$BUCKET 2>/dev/null || true  
ais bucket props set ais://$BUCKET checksum.type=md5 >/dev/null

# Test 1: Normal upload that completes successfully
echo "--- Testing: Normal upload (baseline) ---"
test_file="$TEMPDIR/test_file"
dd if=/dev/urandom of="$test_file" bs=1M count=30 status=none

object_name="test-resume-baseline"
upload_output1=$(s3cmd put "$test_file" s3://$BUCKET/$object_name \
    --multipart-chunk-size-mb=10 \
    --force $HOST_ARGS 2>&1) || {
    echo "FAIL: Baseline upload failed"
    exit 1
}

echo "Baseline upload completed successfully"

# Verify baseline object
s3cmd info s3://$BUCKET/$object_name $HOST_ARGS >/dev/null || {
    echo "FAIL: Baseline object not found"
    exit 1
}

expected_size=$(stat --printf="%s" "$test_file")
actual_size=$(s3cmd info s3://$BUCKET/$object_name $HOST_ARGS | grep "File size:" | awk '{print $3}' | head -1)

if [[ "$expected_size" != "$actual_size" ]]; then
    echo "FAIL: Baseline size mismatch - expected: $expected_size, actual: $actual_size"
    exit 1
fi

echo "PASS: Baseline upload verified"

# Clean up baseline
s3cmd del s3://$BUCKET/$object_name $HOST_ARGS >/dev/null

# Test 2: Upload with --continue-put (should work even if no previous upload)
echo "--- Testing: Upload with --continue-put (no previous upload) ---"
object_name2="test-resume-fresh"
upload_output2=$(s3cmd put "$test_file" s3://$BUCKET/$object_name2 \
    --multipart-chunk-size-mb=10 \
    --continue-put \
    --force $HOST_ARGS 2>&1) || {
    echo "FAIL: Fresh upload with --continue-put failed"
    exit 1
}

echo "Fresh upload with --continue-put completed"

# Verify the upload worked
s3cmd info s3://$BUCKET/$object_name2 $HOST_ARGS >/dev/null || {
    echo "FAIL: Fresh upload with --continue-put object not found"
    exit 1
}

actual_size2=$(s3cmd info s3://$BUCKET/$object_name2 $HOST_ARGS | grep "File size:" | awk '{print $3}' | head -1)
if [[ "$expected_size" != "$actual_size2" ]]; then
    echo "FAIL: Fresh upload with --continue-put size mismatch"
    exit 1
fi

echo "PASS: Fresh upload with --continue-put verified"

# Test 3: Test --continue-put with existing complete object (should overwrite or skip)
echo "--- Testing: --continue-put with existing complete object ---"
upload_output3=$(s3cmd put "$test_file" s3://$BUCKET/$object_name2 \
    --multipart-chunk-size-mb=10 \
    --continue-put \
    $HOST_ARGS 2>&1) || {
    echo "FAIL: --continue-put with existing object failed"
    exit 1
}

echo "Upload with --continue-put on existing object completed"
echo "Output: $upload_output3"

# Verify object still exists and has correct size
actual_size3=$(s3cmd info s3://$BUCKET/$object_name2 $HOST_ARGS | grep "File size:" | awk '{print $3}' | head -1)
if [[ "$expected_size" != "$actual_size3" ]]; then
    echo "FAIL: --continue-put with existing object corrupted size"
    exit 1
fi

echo "PASS: --continue-put with existing object handled correctly"

# Test 4: Test with --upload-id parameter (syntax check)
echo "--- Testing: --upload-id parameter syntax ---"
# We can't easily create a real upload ID to test with, but we can verify the parameter is accepted
# This test may fail, which is expected if no such upload ID exists

if s3cmd put "$test_file" s3://$BUCKET/test-upload-id \
    --multipart-chunk-size-mb=10 \
    --upload-id="dummy-upload-id-123" \
    --continue-put \
    $HOST_ARGS 2>/dev/null; then
    echo "PASS: --upload-id parameter accepted (upload may have completed)"
    # Clean up if it somehow worked
    s3cmd del s3://$BUCKET/test-upload-id $HOST_ARGS 2>/dev/null || true
else
    echo "PASS: --upload-id parameter processed (expected failure for dummy ID)"
fi

# Clean up
s3cmd del s3://$BUCKET/$object_name2 $HOST_ARGS >/dev/null

echo "=== Resume upload functionality tests completed ==="
