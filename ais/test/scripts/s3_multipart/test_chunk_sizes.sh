#!/bin/bash

# Test s3cmd multipart uploads with different chunk sizes
# Validates that multipart is used/not used based on file size vs chunk size

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
    # Clean up any test objects
    s3cmd del s3://$BUCKET/test-* $HOST_ARGS 2>/dev/null || true
}
trap cleanup EXIT

echo "=== Testing s3cmd multipart chunk sizes ==="

# Ensure bucket exists
ais create ais://$BUCKET 2>/dev/null || true
ais bucket props set ais://$BUCKET checksum.type=md5 >/dev/null

# Test cases: [file_size_mb, chunk_size_mb, should_use_multipart]
test_cases=(
    "10 15 false"   # 10MB file, 15MB chunks -> no multipart
    "30 15 true"    # 30MB file, 15MB chunks -> multipart (2 parts)
    "50 10 true"    # 50MB file, 10MB chunks -> multipart (5 parts) 
    "20 5 true"     # 20MB file, 5MB chunks -> multipart (4 parts)
    "3 5 false"     # 3MB file, 5MB chunks -> no multipart
)

for test_case in "${test_cases[@]}"; do
    read -r file_size chunk_size should_multipart <<< "$test_case"
    
    echo "--- Testing: ${file_size}MB file with ${chunk_size}MB chunks (expect multipart: $should_multipart) ---"
    
    # Create test file
    test_file="$TEMPDIR/testfile_${file_size}mb"
    dd if=/dev/urandom of="$test_file" bs=1M count=$file_size status=none
    
    # Upload with specified chunk size
    object_name="test-${file_size}mb-${chunk_size}chunk"
    echo "Uploading $test_file as $object_name..."
    
    upload_output=$(s3cmd put "$test_file" s3://$BUCKET/$object_name \
        --multipart-chunk-size-mb=$chunk_size \
        --force $HOST_ARGS 2>&1) || {
        echo "FAIL: Upload failed for $object_name"
        echo "$upload_output"
        exit 1
    }
    
    # Verify object exists and has correct size
    expected_size=$((file_size * 1024 * 1024))
    info_output=$(s3cmd info s3://$BUCKET/$object_name $HOST_ARGS 2>&1) || {
        echo "FAIL: Cannot get info for uploaded object $object_name"
        exit 1
    }
    
    actual_size=$(echo "$info_output" | grep "File size:" | awk '{print $3}' | head -1)
    if [[ "$actual_size" != "$expected_size" ]]; then
        echo "FAIL: Size mismatch for $object_name - expected: $expected_size, actual: $actual_size"
        exit 1
    fi
    
    # Check if multipart was used based on output patterns
    multipart_used=false
    if echo "$upload_output" | grep -q "multipart\|part.*of.*"; then
        multipart_used=true
    fi
    
    if [[ "$should_multipart" == "true" && "$multipart_used" == "false" ]]; then
        echo "FAIL: Expected multipart for $object_name but it wasn't used"
        echo "Upload output: $upload_output"
        exit 1
    fi
    
    if [[ "$should_multipart" == "false" && "$multipart_used" == "true" ]]; then
        echo "FAIL: Expected single-part upload for $object_name but multipart was used"
        echo "Upload output: $upload_output"
        exit 1
    fi
    
    echo "PASS: $object_name uploaded correctly (multipart used: $multipart_used)"
    
    # Clean up this test object
    s3cmd del s3://$BUCKET/$object_name $HOST_ARGS >/dev/null
    rm "$test_file"
done

echo "=== All chunk size tests passed ==="
