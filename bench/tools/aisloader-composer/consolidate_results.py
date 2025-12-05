"""
Python script designed to analyze and amalgamate the data from
AIS Loader-generated files into a cohesive report

Example Usage:
python consolidate_results.py output/get_batch/ais-bench-1MiB/16 get_batch
python consolidate_results.py output/get/ais-bench-1MiB
python consolidate_results.py output/put/ais-bench-1MiB put

Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
"""

import os
import json
import sys

# Specify the folder where your files are located
if len(sys.argv) not in [2, 3]:
    print("Usage: python consolidate_results.py <folder_path> [operation_type]")
    print("  operation_type: get, put, or get_batch (default: get)")
    sys.exit(1)

FOLDER_PATH = sys.argv[1]
OPERATION = sys.argv[2] if len(sys.argv) == 3 else "get"

if OPERATION not in ["get", "put", "get_batch"]:
    print(
        f"Error: Invalid operation type '{OPERATION}'. Must be 'get', 'put', or 'get_batch'"
    )
    sys.exit(1)

MIN_LATENCY = float("inf")
MAX_LATENCY = 0
SUM_AVG_LATENCY = 0
SUM_THROUGHPUT = 0
SUM_ERRORS = 0
SUM_COUNT = 0
SUM_BYTES = 0
FILE_COUNT = 0


def nanoseconds_to_milliseconds(ns):
    """Convert nanoseconds to milliseconds."""
    return int(ns) / 1_000_000


def bytes_per_second_to_gib_per_second(bps):
    """Convert bytes per second to GiB per second."""
    return int(bps) / (1024**3)


# List files in the folder
if not os.path.exists(FOLDER_PATH):
    print(f"Error: Folder '{FOLDER_PATH}' does not exist!")
    sys.exit(1)

file_list = [
    f for f in os.listdir(FOLDER_PATH) if not f.startswith(".") and f.endswith(".json")
]

if not file_list:
    print(f"Error: No JSON files found in '{FOLDER_PATH}'")
    sys.exit(1)

print(f"Processing {len(file_list)} files from '{FOLDER_PATH}'")
print(f"Operation type: {OPERATION}")
print("-" * 80)

# Process each file
for file_name in file_list:
    file_path = os.path.join(FOLDER_PATH, file_name)

    try:
        # Read and parse JSON file
        with open(file_path, "r", encoding="utf-8") as current_file:
            data = json.load(current_file)

        # Get the last entry (most recent snapshot)
        if not data:
            print(f"Warning: Empty data in {file_name}, skipping...")
            continue

        last_entry = data[-1]

        # Extract metrics for the specified operation
        if OPERATION not in last_entry:
            print(
                f"Warning: Operation '{OPERATION}' not found in {file_name}, skipping..."
            )
            continue

        op_data = last_entry[OPERATION]

        # Extract metrics
        count = int(op_data.get("count", 0))
        bytes_transferred = int(op_data.get("bytes", 0))
        errors = int(op_data.get("errors", 0))
        avg_latency_ns = int(op_data.get("latency", 0))
        min_latency_ns = int(op_data.get("min_latency", 0))
        max_latency_ns = int(op_data.get("max_latency", 0))
        throughput_bps = int(op_data.get("throughput", 0))

        # Skip if no operations were performed
        if count == 0:
            print(f"Warning: No operations in {file_name}, skipping...")
            continue

        # Convert to appropriate units
        avg_latency_ms = nanoseconds_to_milliseconds(avg_latency_ns)
        min_latency_ms = nanoseconds_to_milliseconds(min_latency_ns)
        max_latency_ms = nanoseconds_to_milliseconds(max_latency_ns)
        throughput_gib_s = bytes_per_second_to_gib_per_second(throughput_bps)

        # Aggregate metrics
        MIN_LATENCY = min(MIN_LATENCY, min_latency_ms)
        MAX_LATENCY = max(MAX_LATENCY, max_latency_ms)
        SUM_AVG_LATENCY += avg_latency_ms
        SUM_THROUGHPUT += throughput_gib_s
        SUM_ERRORS += errors
        SUM_COUNT += count
        SUM_BYTES += bytes_transferred
        FILE_COUNT += 1

        print(
            f"✓ {file_name}: {count:,} ops, {throughput_gib_s:.2f} GiB/s, {errors} errors"
        )

    except json.JSONDecodeError as e:
        print(f"Error: Failed to parse JSON in {file_name}: {e}")
        continue
    except Exception as e:
        print(f"Error processing {file_name}: {e}")
        continue

if FILE_COUNT == 0:
    print("\nError: No valid files were processed!")
    sys.exit(1)

# Calculate averages
avg_of_avg_latency = SUM_AVG_LATENCY / FILE_COUNT
avg_throughput = SUM_THROUGHPUT / FILE_COUNT
total_gib = SUM_BYTES / (1024**3)

# Print the consolidated results
print("\n" + "=" * 80)
print("CONSOLIDATED RESULTS")
print("=" * 80)
print(f"Files Processed:                 {FILE_COUNT}")
print(f"Total Operations:                {SUM_COUNT:,}")
print(f"Total Data Transferred:          {total_gib:.2f} GiB")
print(f"Minimum Latency:                 {MIN_LATENCY:.3f} ms")
print(f"Average of Average Latencies:    {avg_of_avg_latency:.3f} ms")
print(f"Maximum Latency:                 {MAX_LATENCY:.3f} ms")
print(f"Average Throughput:              {avg_throughput:.2f} GiB/s")
print(f"Summation of all Throughputs:    {SUM_THROUGHPUT:.2f} GiB/s")
print(f"Total Errors:                    {SUM_ERRORS}")
print("=" * 80)
