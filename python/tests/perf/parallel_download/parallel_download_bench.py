#!/usr/bin/env python3
"""
Benchmark comparing single-stream vs parallel download throughput for one AIS object.

Runs both modes against the same object, discards all data, and prints throughput
in MiB/s plus the parallel speedup multiplier over the single-stream baseline.

Environment variables:
    AIS_ENDPOINT  AIS cluster endpoint          (default: http://localhost:8080)
    AIS_BUCKET    Bucket containing test object  (default: test)
    AIS_OBJECT    Object name to download        (default: testfile.bin)
    AIS_WORKERS   Number of parallel workers     (default: 16)

Usage:
    AIS_ENDPOINT=http://10.52.160.25:51080 \\
    AIS_BUCKET=mpd-bench \\
    AIS_OBJECT=largefile.bin \\
    AIS_WORKERS=8 \\
    python3 python/tests/perf/multipart_download_bench.py
"""

import os
import time

from aistore import Client

ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
BUCKET = os.environ.get("AIS_BUCKET", "test")
OBJECT = os.environ.get("AIS_OBJECT", "testfile.bin")
WORKERS = int(os.environ.get("AIS_WORKERS", "16"))


def benchmark_single_stream():
    """Benchmark single-stream download for baseline."""
    print("Testing single-stream download (baseline)...")

    client = Client(ENDPOINT)
    obj = client.bucket(BUCKET).object(OBJECT)
    size = obj.props.size

    start = time.perf_counter()
    reader = obj.get_reader()

    for chunk in reader:
        pass

    elapsed = time.perf_counter() - start
    throughput = size / elapsed / (1024 * 1024)

    print(f"Throughput: {throughput:.2f} MiB/s")
    return throughput


def benchmark_parallel():
    """Benchmark parallel download with ProcessPoolExecutor + shared memory."""
    print("\nTesting parallel download (ProcessPool + SHM)...")

    client = Client(ENDPOINT)
    obj = client.bucket(BUCKET).object(OBJECT)
    size = obj.props.size

    start = time.perf_counter()
    for chunk in obj.get_reader(num_workers=WORKERS):
        pass

    elapsed = time.perf_counter() - start
    throughput = size / elapsed / (1024 * 1024)

    print(f"Throughput: {throughput:.2f} MiB/s")

    return throughput


def main():
    print("\n" + "=" * 60)
    print("PYTHON PARALLEL DOWNLOAD BENCHMARK")
    print("=" * 60)
    print(f"Endpoint: {ENDPOINT}")
    print(f"Object: {BUCKET}/{OBJECT}")
    print(f"Workers: {WORKERS}")
    print("=" * 60 + "\n")

    single_tp = benchmark_single_stream()
    parallel_tp = benchmark_parallel()

    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Single-stream: {single_tp:7.2f} MiB/s (baseline)")
    print(f"Parallel:      {parallel_tp:7.2f} MiB/s ({parallel_tp/single_tp:.2f}x)")
    print("=" * 60)


if __name__ == "__main__":
    main()
