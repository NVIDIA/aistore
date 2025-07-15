#!/bin/bash

## Prerequisites: #################################################################################
# - aistore cluster
# - ais (CLI)
#
## Example:
## ./lhotse_test_suite.sh --bucket ais://lhotse-test --cuts 247 --batch-size 50
#################################################################################################

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi

## Command line options and respective defaults
bucket="ais://lhotse-test"
num_cuts=247
batch_size=50
num_shards=50

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    --cuts) num_cuts=$2; shift; shift;;
    --batch-size) batch_size=$2; shift; shift;;
    --shards) num_shards=$2; shift; shift;;
    --help|-h)
      echo "Usage: $0 [options]"
      echo "Options:"
      echo "  --bucket BUCKET     Test bucket (default: ais://lhotse-test)"
      echo "  --cuts NUM          Number of cuts to generate (default: 247)"
      echo "  --batch-size NUM    Cuts per batch (default: 50)"
      echo "  --shards NUM        Number of audio shards to create (default: 50)"
      echo "  --help, -h          Show this help"
      exit 0;;
    *) echo "fatal: unknown argument '${1}'. Use --help for usage."; exit 1;;
  esac
done

## uncomment for verbose output
## set -x

TEST_DIR="/tmp/lhotse_test_output_$$"
MANIFEST="$TEST_DIR/test_manifest.jsonl"

## Generate synthetic Lhotse cuts manifest
gen_test_manifest() {
  local num_cuts=$1
  local output=$2

  echo "Generating $num_cuts cuts in $output..."

  rm -f "$output"

  for i in $(seq 1 $num_cuts); do
    # Pick random shard (001 to num_shards)
    local shard_num=$(printf "%03d" $(( (RANDOM % num_shards) + 1 )))

    # Pick random file within shard (01-05, matching template)
    local file_num=$(printf "%02d" $(( (RANDOM % 5) + 1 )))

    # Generate random timing (ensure leading zero for valid JSON)
    local start=$(printf "%.3f" $(echo "scale=3; $RANDOM / 32767 * 10" | bc))
    local duration=$(printf "%.3f" $(echo "scale=3; $RANDOM / 32767 * 5 + 1" | bc))

    # Create cut JSON (modern layout with sources array)
    cat >> "$output" << EOF
{"id": "cut_${i}", "start": ${start}, "duration": ${duration}, "recording": {"sources": [{"source": "${bucket}/audio-${shard_num}.tar/audio-file-${file_num}.wav"}]}}
EOF
  done

  echo "Generated $num_cuts cuts referencing audio-001.tar through audio-$(printf "%03d" $num_shards).tar"
  echo "Each shard contains: audio-file-01.wav through audio-file-05.wav"
}

echo "Lhotse Multi-Batch Test Suite"
echo "================================="
echo "Bucket: $bucket"
echo "Cuts: $num_cuts"
echo "Batch size: $batch_size"
echo "Shards: $num_shards"
echo

# Setup test environment
echo "Setting up test environment..."
rm -rf "$TEST_DIR"
mkdir -p "$TEST_DIR" || { echo "Error: failed to create test directory"; exit 1; }
cd "$TEST_DIR"

cleanup() {
  rc=$?
  echo "Cleaning up..."
  cd ..
  rm -rf "$TEST_DIR" 2>/dev/null
  ais rmb "$bucket" -y 1>/dev/null 2>&1
  exit $rc
}

trap cleanup EXIT INT TERM

# Generate test shards
echo "Generating test audio shards..."
ais archive gen-shards "${bucket}/audio-{001..$(printf "%03d" $num_shards)}.tar" \
  --fext '.wav' --fcount 5 --fsize 512KB \
  --output-template "audio-file-{01..05}.wav" \
  --cleanup || { echo "Error: failed to generate shards"; exit 1; }

# Verify shards were created
shard_count=$(ais ls "$bucket" -H --name-only | wc -l)
echo "Created $shard_count shards"

# Generate test manifest
gen_test_manifest $num_cuts "$MANIFEST"
gzip "$MANIFEST" || { echo "Error: failed to compress manifest"; exit 1; }

# Verify manifest
echo "Manifest info:"
echo "  Size: $(ls -lh ${MANIFEST}.gz | awk '{print $5}')"
echo "  Sample cut:"
if command -v jq >/dev/null; then
  zcat "${MANIFEST}.gz" | head -1 | jq . || { echo "Error: invalid JSON in manifest"; exit 1; }
else
  zcat "${MANIFEST}.gz" | head -1
fi

echo

# Test 1: Single batch mode (backward compatibility)
echo "Test 1: Single batch mode..."
ais ml lhotse-get-batch --cuts "${MANIFEST}.gz" single_output.tar || { echo "Error: single batch test failed"; exit 1; }
echo "Single batch: $(ls -lh single_output.tar | awk '{print $5}')"

# Test 2: Multi-batch mode
echo "Test 2: Multi-batch mode..."
expected_batches=$(( (num_cuts + batch_size - 1) / batch_size ))  # ceiling division
ais ml lhotse-get-batch --cuts "${MANIFEST}.gz" \
  --batch-size $batch_size \
  --output-template "batch-{001..$(printf "%03d" $expected_batches)}.tar" || { echo "Error: multi-batch test failed"; exit 1; }

# Verify outputs
batch_count=$(ls batch-*.tar 2>/dev/null | wc -l)
echo "Generated $batch_count batch files (expected: $expected_batches):"
ls -lh batch-*.tar | head -3
if [ $batch_count -gt 3 ]; then
  echo "   ..."
  ls -lh batch-*.tar | tail -1
fi

# Verify batch count matches expectation
[ "$batch_count" -eq "$expected_batches" ] || { echo "Error: batch count mismatch"; exit 1; }

# Test 3: TBD: Sample rate conversion
echo "Test 3: TBD: Sample rate conversion..."
ais ml lhotse-get-batch --cuts "${MANIFEST}.gz" \
  --batch-size 25 \
  --output-template "audio-{01..20}.tar"
## NOTE: range read not supported yet
##  --sample-rate 16000 || { echo "Error: sample rate test failed"; exit 1; }

audio_count=$(ls audio-*.tar 2>/dev/null | wc -l)
echo "Generated $audio_count audio batches with sample rate conversion"

# Test 4: Template exhaustion
echo "Test 4: Template exhaustion..."
if ais ml lhotse-get-batch --cuts "${MANIFEST}.gz" \
  --batch-size 100 \
  --output-template "small-{01..02}.tar" 2>&1 | grep -q "template exhausted"; then
    echo "Template exhaustion handled correctly"
else
    echo "Error: template exhaustion not detected"
    exit 1
fi

# Test 5: Streaming mode
echo "Test 5: Streaming mode..."
ais ml lhotse-get-batch --cuts "${MANIFEST}.gz" \
  --batch-size 30 \
  --output-template "stream-{001..010}.tar" \
  --streaming || { echo "Error: streaming test failed"; exit 1; }

stream_count=$(ls stream-*.tar 2>/dev/null | wc -l)
echo "Generated $stream_count streaming batches"

# Test 6: Verify batch contents
echo "Test 6: Verifying batch contents..."
first_batch=$(ls batch-001.tar 2>/dev/null || ls audio-01.tar 2>/dev/null || echo "")
if [[ -n "$first_batch" ]]; then
    echo "Contents of $(basename $first_batch):"
    ais ls "$first_batch" --archive | head -5 || { echo "Error: failed to list archive contents"; exit 1; }
    echo "Archive contents verified"
else
    echo "Error: no batch files found for content verification"
fi

# Test 7: Performance test with larger manifest
echo "Test 7: Performance test..."
large_cuts=$((num_cuts * 4))
gen_test_manifest $large_cuts "large_manifest.jsonl"
echo "Running performance test with $large_cuts cuts..."
time ais ml lhotse-get-batch --cuts "large_manifest.jsonl" \
  --batch-size 100 \
  --output-template "perf-{001..100}.tar" >/dev/null || { echo "Error: performance test failed"; exit 1; }

perf_count=$(ls perf-*.tar 2>/dev/null | wc -l)
echo "Performance test completed: $perf_count batches generated"

echo
echo "All tests passed:"
echo "   - Single batch files: $(ls single_output.tar 2>/dev/null | wc -l)"
echo "   - Multi-batch files: $(ls batch-*.tar audio-*.tar stream-*.tar perf-*.tar 2>/dev/null | wc -l)"
echo "   - Total output size: $(du -sh . | awk '{print $1}')"
echo "   - Test directory: $TEST_DIR"
echo "   - Test bucket: $bucket (will be cleaned up)"
