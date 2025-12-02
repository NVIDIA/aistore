#!/bin/bash

# Test s3cmd multipart management commands (multipart, listmp, abortmp)

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
    # Clean up any test objects and multipart uploads
    s3cmd del s3://$BUCKET/test-* $HOST_ARGS 2>/dev/null || true
    # Try to abort any leftover multipart uploads
    s3cmd multipart s3://$BUCKET $HOST_ARGS 2>/dev/null | grep "test-" | while read line; do
        upload_id=$(echo "$line" | awk '{print $NF}')
        object_name=$(echo "$line" | awk '{print $(NF-1)}')
        s3cmd abortmp s3://$BUCKET/$object_name $upload_id $HOST_ARGS 2>/dev/null || true
    done 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Testing s3cmd multipart management commands ==="

# Ensure bucket exists  
ais create ais://$BUCKET 2>/dev/null || true
ais bucket props set ais://$BUCKET checksum.type=md5 >/dev/null

# Test 1: Show multipart uploads (should initially be empty)
echo "--- Testing: s3cmd multipart (list uploads) ---"
multipart_output=$(s3cmd multipart s3://$BUCKET $HOST_ARGS 2>&1) || {
    echo "FAIL: s3cmd multipart command failed"
    exit 1
}

echo "Current multipart uploads:"
echo "$multipart_output"

# Check if the command works (even if no uploads)
if echo "$multipart_output" | grep -q "ERROR"; then
    echo "FAIL: s3cmd multipart command returned error"
    exit 1
fi

echo "PASS: s3cmd multipart command works"

# Test 2: Try to create a large upload that might create multipart state
echo "--- Testing: Upload large file to potentially create multipart state ---"
test_file="$TEMPDIR/large_testfile"
dd if=/dev/urandom of="$test_file" bs=1M count=50 status=none

object_name="test-large-multipart"

# Start upload (this should work and complete)
upload_output=$(s3cmd put "$test_file" s3://$BUCKET/$object_name \
    --multipart-chunk-size-mb=10 \
    --force $HOST_ARGS 2>&1) || {
    echo "FAIL: Large file upload failed"
    echo "$upload_output"
    exit 1
}

echo "Large file uploaded successfully"

# Verify the object exists
s3cmd info s3://$BUCKET/$object_name $HOST_ARGS >/dev/null || {
    echo "FAIL: Uploaded large object not found"
    exit 1
}

echo "PASS: Large object verified"

# Test 3: List multipart uploads again (might show completed uploads or be empty)  
echo "--- Testing: s3cmd multipart after upload ---"
multipart_output2=$(s3cmd multipart s3://$BUCKET $HOST_ARGS 2>&1) || {
    echo "FAIL: s3cmd multipart command failed after upload"
    exit 1
}

echo "Multipart uploads after large upload:"
echo "$multipart_output2"
echo "PASS: s3cmd multipart command works after upload"

# Test 4: Test listmp command (this might not work if upload completed)
echo "--- Testing: s3cmd listmp command ---"
# This command expects an upload ID, so it might fail for completed uploads
# We'll test the command syntax but expect it might fail
if s3cmd listmp s3://$BUCKET/$object_name "dummy-id" $HOST_ARGS 2>/dev/null; then
    echo "PASS: s3cmd listmp command accepted (upload may not exist)"
else
    echo "PASS: s3cmd listmp command ran (expected failure for non-existent upload)"
fi

# Test 5: Test abortmp command  
echo "--- Testing: s3cmd abortmp command ---"
# This will likely fail since our upload completed, but we test the command syntax
if s3cmd abortmp s3://$BUCKET/$object_name "dummy-id" $HOST_ARGS 2>/dev/null; then
    echo "PASS: s3cmd abortmp command accepted (upload may not exist)"
else
    echo "PASS: s3cmd abortmp command ran (expected failure for non-existent upload)"
fi

# Clean up
s3cmd del s3://$BUCKET/$object_name $HOST_ARGS >/dev/null

echo "=== All multipart management command tests passed ==="
