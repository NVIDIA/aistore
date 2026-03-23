"""
Benchmark: AISMapDataset vs AISParallelMapDataset (parallel download).

Usage:
    AIS_ENDPOINT=http://... AIS_BUCKET=... python3 multipart_download.py

Optional env vars:
    AIS_PREFIX    object name prefix filter      (default: "")
    AIS_WORKERS   parallel workers per object    (default: 16)
    BATCH_SIZE    samples per batch              (default: 4)
    NUM_BATCHES   batches to measure             (default: all)
"""

import os
import statistics
import time

import torch
from torch.utils.data import DataLoader
from aistore import Client
from aistore.pytorch import AISMapDataset, AISParallelMapDataset

ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
BUCKET = os.environ.get("AIS_BUCKET", "bench")
PREFIX = os.environ.get("AIS_PREFIX", "")
PAR_WORKERS = int(os.environ.get("AIS_WORKERS", "16"))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "4"))
_nb_env = os.environ.get("NUM_BATCHES", "")
NUM_BATCHES = int(_nb_env) if _nb_env else None  # None = all batches
SEED = 42

client = Client(ENDPOINT)
bck = client.bucket(BUCKET)
prefix_map = {bck: PREFIX} if PREFIX else {}

# -- Object metadata --------------------------------------------------------
entries = bck.list_all_objects(prefix=PREFIX, props="name,size")
obj_sizes = [e.size for e in entries]
total_gib = sum(obj_sizes) / 1024**3
avg_gib = total_gib / len(obj_sizes) if obj_sizes else 0
min_gib = min(obj_sizes) / 1024**3 if obj_sizes else 0
max_gib = max(obj_sizes) / 1024**3 if obj_sizes else 0

TOTAL_BATCHES = len(obj_sizes) // BATCH_SIZE
if NUM_BATCHES is None:
    NUM_BATCHES = TOTAL_BATCHES

print(f"Bucket: ais://{BUCKET}  prefix={PREFIX!r}")
print(
    f"Objects: {len(obj_sizes)}  total={total_gib:.1f} GiB"
    f"  avg={avg_gib:.2f} GiB  min={min_gib:.2f} GiB  max={max_gib:.2f} GiB"
)
print(
    f"Config:  batch_size={BATCH_SIZE}  num_batches={NUM_BATCHES}/{TOTAL_BATCHES}"
    f"  parallel_workers={PAR_WORKERS}"
)


def close_batch(batch):
    """Release any ParallelBuffer objects held in the batch."""
    for _, content in batch:
        if hasattr(content, "close"):
            content.close()


def bench(label, parallel):
    if parallel:
        ds = AISParallelMapDataset(bck, prefix_map=prefix_map, num_workers=PAR_WORKERS)
    else:
        ds = AISMapDataset(bck, prefix_map=prefix_map)
    g = torch.Generator().manual_seed(SEED)
    loader = DataLoader(
        ds, batch_size=BATCH_SIZE, shuffle=True, generator=g, collate_fn=lambda x: x
    )
    it = iter(loader)

    print(f"\n── {label}")
    total_bytes, total_secs = 0, 0.0
    latencies = []
    for i in range(NUM_BATCHES):
        t0 = time.monotonic()
        batch = next(it)
        elapsed = time.monotonic() - t0

        nb = sum(len(c) for _, c in batch)
        close_batch(batch)

        total_bytes += nb
        total_secs += elapsed
        latencies.append(elapsed)
        print(
            f"  batch {i+1}/{NUM_BATCHES}: {elapsed:.2f}s  {nb/elapsed/1024**2:.0f} MiB/s"
        )

    throughput = total_bytes / total_secs / 1024**3
    samples_sec = NUM_BATCHES * BATCH_SIZE / total_secs
    sorted_lat = sorted(latencies)
    p95_idx = min(int(len(sorted_lat) * 0.95), len(sorted_lat) - 1)

    print(
        f"  throughput: {throughput:.2f} GiB/s"
        f"  samples/s: {samples_sec:.2f}"
        f"  wall: {total_secs:.1f}s"
    )
    print(
        f"  latency  mean={statistics.mean(latencies):.2f}s"
        f"  median={statistics.median(latencies):.2f}s"
        f"  p95={sorted_lat[p95_idx]:.2f}s"
        f"  first={latencies[0]:.2f}s"
    )

    return {
        "throughput": throughput,
        "samples_sec": samples_sec,
        "wall": total_secs,
        "mean": statistics.mean(latencies),
        "median": statistics.median(latencies),
        "p95": sorted_lat[p95_idx],
        "first": latencies[0],
    }


base = bench("GET  (AISMapDataset)", parallel=False)
par = bench(f"Parallel  (AISParallelMapDataset, workers={PAR_WORKERS})", parallel=True)

ROW = "{:<22s}  {:>10s}  {:>10s}  {:>8s}"
print("\n" + ROW.format("", "GET", "Parallel", "Speedup"))
print("─" * 58)
for key, label in [
    ("throughput", "Throughput (GiB/s)"),
    ("samples_sec", "Samples/sec"),
    ("wall", "Total wall time (s)"),
    ("mean", "Batch latency mean (s)"),
    ("median", "Batch latency med (s)"),
    ("p95", "Batch latency p95 (s)"),
    ("first", "Time-to-first-batch (s)"),
]:
    g, p = base[key], par[key]
    if key in ("throughput", "samples_sec"):
        spd = f"{p / g:.1f}x" if g > 0 else "n/a"
    else:
        spd = f"{g / p:.1f}x" if p > 0 else "n/a"
    print(ROW.format(label, f"{g:.2f}", f"{p:.2f}", spd))
