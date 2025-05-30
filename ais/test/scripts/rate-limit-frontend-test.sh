#!/bin/bash

## Frontend Rate Limiting Test for AIS
## Introduced in AIS v3.28
## Tests client-facing request limits by making rapid PUT requests
## and validating that requests are blocked when limits are exceeded.

bucket="ais://rate-limit-frontend-test"

show_usage() {
    echo "Usage: $0 [--bucket BUCKET] [--help]"
    echo ""
    echo "  --bucket BUCKET   Test bucket (default: ais://rate-limit-frontend-test)"
    echo "  --help            Show this help"
}

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    --help) show_usage; exit 0;;
    *) echo "fatal: unknown argument '${1}'"; echo "Use --help for usage"; exit 1;;
  esac
done

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not found" >&2
  exit 1
fi

echo "=== Frontend Rate Limiting Test ==="
echo "Test bucket: $bucket"
echo

## Check if test bucket already exists, create if needed
bucket_exists=true
ais show bucket $bucket -c 1>/dev/null 2>&1 || bucket_exists=false
if [[ "$bucket_exists" == "false" ]]; then
    echo "Creating test bucket: $bucket"
    ais create $bucket || exit $?
fi

cleanup() {
  rc=$?
  echo
  echo "=== Cleanup ==="
  
  echo "Disabling rate limiting..."
  ais bucket props set $bucket rate_limit.frontend.enabled=false 1>/dev/null 2>&1
  
  echo "Cleaning up test objects..."
  ais rmo "$bucket/rate-test-*" -y 1>/dev/null 2>&1
  
  # Only remove bucket if we created it
  if [[ "$bucket_exists" == "false" ]]; then
     echo "Removing test bucket: $bucket"
     ais rmb $bucket -y 1>/dev/null 2>&1
  fi
  
  exit $rc
}

trap cleanup EXIT INT TERM

echo "Checking cluster rate limit configuration..."
# Show default cluster-wide rate limiting settings
ais config cluster rate_limit --json | head -20

echo
echo "=== Frontend Rate Limiting Test ==="
echo "Configuring frontend rate limiting..."

# Frontend rate limiting controls client-facing requests
# Use pipeline-compatible settings: maxTokens=32 (Hard-coded max), burst_size=10
ais bucket props set $bucket rate_limit.frontend.enabled=true rate_limit.frontend.interval=10s rate_limit.frontend.burst_size=10 rate_limit.frontend.max_tokens=32 || exit $?

echo "Frontend rate limiting configured:"
ais bucket props show $bucket rate_limit.frontend

echo
echo "Testing rate limiting enforcement..."
echo "Making rapid requests (should trigger rate limiting)..."

# Hardcoded proxy URL (localhost:8080)
proxy_url="http://127.0.0.1:8080"
echo "Using AIS proxy: $proxy_url"

# Extract bucket name from ais://bucket format
bucket_name=${bucket#ais://}

# Test rate limiting by making rapid PUT requests using curl (no retry logic)
success_count=0
rate_limited_count=0

for i in {1..15}; do
    # Use curl to PUT directly to AIS API without retry logic
    response=$(curl -s -w "%{http_code}" -o /dev/null \
        -X PUT "$proxy_url/v1/objects/$bucket_name/rate-test-$i" \
        -d "test-content-$i")
    
    if [ "$response" = "200" ]; then
        echo "Request $i: SUCCESS"
        ((success_count++))
    elif [ "$response" = "429" ]; then
        echo "Request $i: RATE LIMITED (429)"
        ((rate_limited_count++))
    else
        echo "Request $i: ERROR ($response)"
    fi
done

echo
echo "Results: $success_count successful, $rate_limited_count rate-limited"

# Validate that rate limiting is working
if [[ $rate_limited_count -gt 0 ]]; then
    echo "Frontend rate limiting is working! Detected $rate_limited_count HTTP 429 responses."
else
    echo "No rate limiting detected (no HTTP 429 responses)"
    echo "Note: Using curl to bypass CLI retry logic and detect actual rate limiting"
fi

echo
echo "=== Frontend Rate Limiting Test Completed ===" 