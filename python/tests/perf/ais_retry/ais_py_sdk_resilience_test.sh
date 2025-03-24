#!/bin/bash

# AIStore Python SDK Resilience Test
# -----------------------------------
# This script tests AIStore cluster resilience by simulating target maintenance cycles, 
# node restarts, or version upgrades while a Python script performs multi-threaded object reads.

set -e  # Exit immediately on error
set -o pipefail  # Fail if any command in a pipeline fails
set -u  # Treat unset variables as an error

# Configuration
BUCKET_NAME="resil-test"
BUCKET="ais://$BUCKET_NAME"
OBJECT_COUNT=30
OBJECT_SIZE=20MB
PYTHON_SCRIPT="./test_object_read_during_restart.py"
THREAD_COUNT=6
SLEEP_INTERVAL=10  # Time to wait for cluster stabilization

# Ensure `ais` CLI is installed
if ! command -v ais &> /dev/null; then
    echo "Error: AIStore CLI ('ais') is not installed." >&2
    exit 1
fi

# Verify AIStore cluster is running
if ! ais show cluster &> /dev/null; then
    echo "Error: AIStore cluster is not running." >&2
    exit 1
fi

# Verify minimum target count
TARGET_COUNT=$(ais show cluster target | awk 'NR>1' | wc -l)
if [ "$TARGET_COUNT" -lt 3 ]; then
    echo "Error: At least 3 targets are required for resilience testing (found $TARGET_COUNT)." >&2
    exit 1
fi

# Cleanup any existing bucket (ignores errors)
ais rmb -y "$BUCKET" &> /dev/null || true

# Create test bucket
echo "Creating test bucket: $BUCKET"
ais bucket create "$BUCKET" || exit 1

# Generate test objects
echo "Generating $OBJECT_COUNT test objects..."
for i in $(seq 1 "$OBJECT_COUNT"); do
    OBJECT_NAME="object-$(uuidgen | tr -d '-')"
    dd if=/dev/urandom of="$OBJECT_NAME" bs=$OBJECT_SIZE count=1 status=none
    ais put "$OBJECT_NAME" "$BUCKET" &> /dev/null || exit 1
    rm -f "$OBJECT_NAME"
done

# Retrieve target node list (excluding headers)
TARGETS=($(ais show cluster target | awk 'NR>1 {print $1}'))

# Start Python test script in the background
echo "Starting Python test script..."
THREAD_COUNT=$THREAD_COUNT AIS_BUCKET=$BUCKET_NAME python3 "$PYTHON_SCRIPT" > test.log 2>&1 &
PYTHON_PID=$!
echo "Python script started with PID: $PYTHON_PID"

# Function to check active thread count from log
check_threads() {
    local last_entry
    last_entry=$(grep "Active Threads:" test.log | tail -n 1)

    # Extract active thread count using awk
    local active_threads
    active_threads=$(echo "$last_entry" | awk -F 'Active Threads: ' '{print $2}' | awk -F '|' '{print $1}' | tr -cd '[:digit:]')

    # Return success if all threads are active
    [[ "$active_threads" -eq "$THREAD_COUNT" ]]
}

# Function to wait for rebalance completion
wait_for_rebalance() {
    while ais show rebalance | grep -q "Running"; do
        echo "Waiting for rebalance to complete..."
        sleep "$SLEEP_INTERVAL"
    done
}

# Main test loop for target maintenance cycles
for target in "${TARGETS[@]}"; do
    echo -e "\nTesting maintenance cycle for target: $target"

    # This delay is added to simulate the longer smap update times typically seen in larger clusters.
    # On a local cluster, smap updates happen almost instantly, so 404 errors (ErrObjNotFound) are rare.
    # In production, such errors may occur during rebalancing or when there is a delay in updating the smap.
    echo "Disabling rebalance..."
    ais config cluster set rebalance.enabled false &> /dev/null

    echo "Starting maintenance on target: $target"
    if ! ais cluster add-remove-nodes start-maintenance "$target" -y; then
        echo "Error: Failed to start maintenance on target: $target" >&2
        exit 1
    fi

    echo "Waiting $SLEEP_INTERVAL seconds for cluster updates..."
    sleep "$SLEEP_INTERVAL"

    echo "Enabling rebalance..."
    ais config cluster set rebalance.enabled true &> /dev/null

    wait_for_rebalance

    echo "Stopping maintenance on target: $target"
    if ! ais cluster add-remove-nodes stop-maintenance "$target" -y; then
        echo "Error: Failed to stop maintenance on target: $target" >&2
        exit 1
    fi

    sleep "$SLEEP_INTERVAL"
    wait_for_rebalance

    # Validate Python script status
    if ! ps -p "$PYTHON_PID" > /dev/null; then
        echo "Error: Python script terminated unexpectedly!" >&2
        kill "$PYTHON_PID"
        cat test.log | grep "ERROR"
        exit 1
    fi

    if ! check_threads; then
        echo "Error: Thread count dropped during maintenance cycle!" >&2
        kill "$PYTHON_PID"
        cat test.log | grep "ERROR"
        exit 1
    fi
done

# Final verification
echo -e "\nFinal verification:"
if ps -p "$PYTHON_PID" > /dev/null && check_threads; then
    echo "Test PASSED: All threads remained active during maintenance cycles."
    kill "$PYTHON_PID"
    exit 0
else
    echo "Test FAILED: Some threads terminated during maintenance cycles." >&2
    exit 1
fi
