#!/usr/bin/env python3
"""
Single-object parallel download grid benchmark.

Downloads one large object with varying worker counts and reports throughput.

For accurate results that reflect disk performance rather than page-cache hits,
drop the Linux page cache on every target node between runs:

    $ sync && echo 3 | sudo tee /proc/sys/vm/drop_caches

This script does NOT drop the cache automatically — the method varies by
deployment (bare-metal SSH, Kubernetes exec, etc.).  The benchmark results
in the blog post were produced with page cache dropped before each measurement.

Environment variables:
    AIS_ENDPOINT    AIS cluster endpoint          (default: http://localhost:8080)
    AIS_BUCKET      Bucket containing test object  (default: test)
    AIS_OBJECT      Object name to download        (default: testfile.bin)
    AIS_WORKERS     Comma-separated worker counts  (default: 1,2,4,8,16,32,48,64)

Usage:
    AIS_ENDPOINT=http://10.52.160.25:51080 \
    AIS_BUCKET=mpd-bench \
    AIS_OBJECT=random_128gib_mono.bin \
    AIS_WORKERS=1,2,4,8,16,32,48,64 \
    python3 python/tests/perf/single_object_grid_bench.py
"""

import os
import time

from aistore import Client

ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
BUCKET = os.environ.get("AIS_BUCKET", "test")
OBJECT = os.environ.get("AIS_OBJECT", "testfile.bin")
WORKER_LIST = [
    int(w) for w in os.environ.get("AIS_WORKERS", "1,2,4,8,16,32,48,64").split(",")
]

client = Client(ENDPOINT)
obj = client.bucket(BUCKET).object(OBJECT)
size = obj.props.size
gib = size / 1024**3


def bench_get(num_workers):
    """Download the object, discard all data, return throughput in MiB/s."""
    if num_workers == 1:
        reader = obj.get_reader()
    else:
        reader = obj.get_reader(num_workers=num_workers)

    t0 = time.monotonic()
    for _ in reader:
        pass
    elapsed = time.monotonic() - t0
    return size / elapsed / 2**20, elapsed


print(f"Object: ais://{BUCKET}/{OBJECT}  ({gib:.1f} GiB)")
print(f"Workers: {WORKER_LIST}\n")

ROW = "{:>8s}  {:>10s}  {:>10s}  {:>8s}"
print(ROW.format("workers", "MiB/s", "time (s)", "speedup"))
print("─" * 42)

baseline = None
for nw in WORKER_LIST:
    mib_s, elapsed = bench_get(nw)
    if baseline is None:
        baseline = mib_s
    spd = f"{mib_s / baseline:.1f}x"
    print(ROW.format(str(nw), f"{mib_s:.0f}", f"{elapsed:.1f}", spd))
