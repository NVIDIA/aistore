"""
Benchmark: AISMapDataset vs AISParallelMapDataset (parallel download).

Usage:
    AIS_ENDPOINT=http://... AIS_BUCKET=... python3 multipart_download.py

Optional env vars:
    AIS_PREFIX    object name prefix filter      (default: "")
    AIS_WORKERS   parallel workers per object    (default: 16)
    BATCH_SIZE    samples per batch              (default: 4)
    NUM_BATCHES   batches to measure             (default: 5)
"""

import os
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
NUM_BATCHES = int(os.environ.get("NUM_BATCHES", "5"))
SEED = 42

client = Client(ENDPOINT)
bck = client.bucket(BUCKET)
prefix_map = {bck: PREFIX} if PREFIX else {}


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
    for i in range(NUM_BATCHES):
        t0 = time.monotonic()
        batch = next(it)
        elapsed = time.monotonic() - t0

        nb = sum(len(c) for _, c in batch)
        close_batch(batch)

        total_bytes += nb
        total_secs += elapsed
        print(
            f"  batch {i+1}/{NUM_BATCHES}: {elapsed:.2f}s  {nb/elapsed/1024**2:.0f} MiB/s"
        )

    avg = total_bytes / total_secs / 1024**2
    print(f"  avg: {avg:.0f} MiB/s")
    return avg


print(
    f"bucket={BUCKET}  prefix={PREFIX!r}  batch={BATCH_SIZE}  workers={PAR_WORKERS}\n"
)
base = bench("GET  (AISMapDataset)", parallel=False)
par = bench(f"Parallel  (AISParallelMapDataset, workers={PAR_WORKERS})", parallel=True)

print(f"\n{'GET':>10}  {base:>7.0f} MiB/s")
print(f"{'Parallel':>10}  {par:>7.0f} MiB/s  ({par/base:.1f}x)")
