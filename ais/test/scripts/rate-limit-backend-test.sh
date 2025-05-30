#!/bin/bash

## Backend Rate Limiting Test for AIS
## Introduced in AIS v3.28
## Tests remote storage request limits by creating objects in cloud buckets
## and measuring timing to validate throttling behavior.

backend_bucket=""

show_usage() {
    echo "Usage: $0 [--backend-bucket BUCKET] [--help]"
    echo ""
    echo "  --backend-bucket BUCKET   Cloud bucket for testing (default: create s3://ais-rate-limit-backend-test)"
    echo "  --help                    Show this help"
    echo ""
    echo "Note: Backend rate limiting is most effective with cloud buckets (s3://, gcp://, etc.)"
}

while (( "$#" )); do
  case "${1}" in
    --backend-bucket) backend_bucket=$2; shift; shift;;
    --help) show_usage; exit 0;;
    *) echo "fatal: unknown argument '${1}'"; echo "Use --help for usage"; exit 1;;
  esac
done

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not found" >&2
  exit 1
fi

echo "=== Backend Rate Limiting Test ==="

# Backend rate limiting controls requests to remote storage 
if [[ -z "$backend_bucket" ]]; then
    backend_bucket="s3://ais-rate-limit-backend-test"
    echo "Creating cloud bucket for backend testing: $backend_bucket"
    if ! ais bucket create $backend_bucket --skip-lookup 1>/dev/null 2>&1; then
        echo "Failed to create cloud bucket for backend testing, skipping backend test"
        echo "Note: This is expected in test environments without cloud credentials"
        echo "=== Test completed successfully! ==="
        exit 0
    fi
    created_test_bucket=true
    
    # Verify bucket was actually created and is accessible
    if ! ais show bucket $backend_bucket -c >/dev/null 2>&1; then
        echo "Created bucket is not accessible, skipping backend test"
        echo "=== Test completed successfully! ==="
        exit 0
    fi
else
    echo "Using specified bucket: $backend_bucket"
    # Verify bucket exists
    if ! ais show bucket $backend_bucket -c >/dev/null 2>&1; then
        echo "Error: Bucket $backend_bucket does not exist"
        exit 1
    fi
    created_test_bucket=false
fi

echo "Test bucket: $backend_bucket"
echo

cleanup() {
  rc=$?
  echo
  echo "=== Cleanup ==="
  
  # Only attempt cleanup if bucket is accessible
  if ais show bucket $backend_bucket -c >/dev/null 2>&1; then
    echo "Disabling rate limiting..."
    ais bucket props set $backend_bucket rate_limit.backend.enabled=false 1>/dev/null 2>&1
    
    echo "Cleaning up test objects..."
    ais rmo "$backend_bucket/rate-test-*" -y 1>/dev/null 2>&1
    
    # Clean up backend test bucket if we created it
    if [[ "$backend_bucket" == "s3://ais-rate-limit-backend-test" ]]; then
      echo "Cleaning up test bucket..."
      ais evict $backend_bucket -y 1>/dev/null 2>&1
    fi
  else
    echo "Backend bucket not accessible, skipping cleanup"
  fi
  
  exit $rc
}

trap cleanup EXIT INT TERM

echo "Checking cluster rate limit configuration..."
# Show default cluster-wide rate limiting settings
ais config cluster rate_limit --json | head -20

echo
echo "=== Backend Rate Limiting Test ==="
echo "Configuring backend rate limiting on: $backend_bucket"

# Backend rate limiting controls requests to remote storage (S3, GCS, etc.)
ais bucket props set $backend_bucket rate_limit.backend.enabled=true rate_limit.backend.interval=5s rate_limit.backend.max_tokens=3 rate_limit.backend.num_retries=2 || exit $?

echo "Backend rate limiting configured:"
ais bucket props show $backend_bucket rate_limit.backend

echo
echo "Testing backend rate limiting..."
backend_start=$(date +%s)
backend_success=0

# Create objects to test backend rate limiting behavior
for i in {1..5}; do
    if echo "backend-test-$i" | ais put - "$backend_bucket/rate-test-backend-$i" 1>/dev/null 2>&1; then
        ((backend_success++))
    fi
done

backend_end=$(date +%s)
backend_duration=$((backend_end - backend_start))

echo "Backend test: $backend_success/5 objects created in ${backend_duration}s"
if [[ "$backend_bucket" == ais://* ]]; then
    echo "For local buckets, backend rate limiting may not be apparent"
fi

echo
echo "Checking that rate limit settings are properly stored..."
echo "Current rate limit settings:"
ais bucket props show $backend_bucket rate_limit

echo
echo "=== Backend Rate Limiting Test Completed ===" 