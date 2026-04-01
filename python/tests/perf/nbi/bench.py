#!/usr/bin/env python3
"""
NBI latency-vs-scale benchmark.

Compares three listing paths at increasing object counts:
  - Regular  : AIS list-objects, walks S3 on every call
  - NBI      : AIS inventory-backed listing, reads local chunks
  - S3 Direct: boto3 ListObjectsV2, client walks S3 directly

Usage:
    export AIS_ENDPOINT=http://<proxy>:51080
    python3 bench.py [--runs 20] [--bucket my-bucket] [--provider aws]
"""

import argparse
import os
import statistics
import time
from typing import Any, Optional

import boto3

from aistore.sdk import Bucket, Client

# ── Configuration ─────────────────────────────────────────────────────────────

SCALE_POINTS = [
    ("bench/1k/", 1_000, " 1K"),
    ("bench/2k/", 2_000, " 2K"),
    ("bench/5k/", 5_000, " 5K"),
    ("bench/10k/", 10_000, "10K"),
    ("bench/20k/", 20_000, "20K"),
    ("bench/40k/", 40_000, "40K"),
    ("bench/50k/", 50_000, "50K"),
    ("bench/80k/", 80_000, "80K"),
]

INVENTORY_WAIT = 600  # seconds
TABLE_WIDTH = 122  # table width

# ── Helpers ───────────────────────────────────────────────────────────────────


def _pcts(data: list[float]) -> tuple[float, float, float, float]:
    """Return (p50, p95, p99, stddev) in seconds."""
    s = sorted(data)
    n = len(s)
    p50 = statistics.median(s)
    p95 = s[min(int(0.95 * n), n - 1)]
    p99 = s[min(int(0.99 * n), n - 1)]
    sd = statistics.stdev(s) if n > 1 else 0.0
    return p50, p95, p99, sd


def _ms(secs: float) -> str:
    """Format seconds as an integer millisecond string."""
    return f"{secs * 1000:.0f}"


def _list_ais(bck: Bucket, prefix: str, inv_name: Optional[str] = None) -> float:
    """Time one AIS list_all_objects call (regular or NBI)."""
    t0 = time.perf_counter()
    bck.list_all_objects(prefix=prefix, inventory_name=inv_name)
    return time.perf_counter() - t0


def _list_s3(s3: Any, bucket: str, prefix: str) -> float:
    """Time a full boto3 ListObjectsV2 pagination loop."""
    t0, token = time.perf_counter(), None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kw["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kw)
        token = resp.get("NextContinuationToken")
        if not token:
            break
    return time.perf_counter() - t0


def _create_inventory(client: Client, bck: Bucket, prefix: str) -> tuple[str, float]:
    """
    Create an NBI inventory and wait for it to finish.
    Returns (inv_name, create_sec) where create_sec is the actual server-side
    job duration from job.get_total_time() — no polling jitter.
    """
    job_id = bck.create_inventory(prefix=prefix, force=True)
    job = client.job(job_id)
    job.wait(timeout=INVENTORY_WAIT, verbose=False)
    create_sec = job.get_total_time().total_seconds()
    inv_name = next(iter(bck.show_inventory().values())).name
    return inv_name, create_sec


# ── Benchmark ─────────────────────────────────────────────────────────────────


# pylint: disable=too-many-locals
def run(client: Client, bck: Bucket, s3: Any, bucket: str, runs: int) -> None:
    """Run the latency-vs-scale benchmark and print a results table to stdout."""
    # one warmup S3 listing to establish a persistent connection
    _list_s3(s3, bucket, SCALE_POINTS[0][0])

    print(f"\n{'='*TABLE_WIDTH}")
    print(f"NBI latency-vs-scale  ({runs} runs each)")
    print(f"{'='*TABLE_WIDTH}")
    hdr1 = (
        f"{'Objects':<9} {'Creation':>10}  "
        f"{'--- Regular (ms) ---':^38}  "
        f"{'--- NBI (ms) ---':^38}  "
        f"{'--- S3 Direct (ms) ---':^38}  "
        f"{'Speedup':>7}"
    )
    hdr2 = (
        f"{'':<9} {'':<10}  "
        f"{'p50':>8} {'p95':>8} {'p99':>8} {'sd':>8}  "
        f"{'p50':>8} {'p95':>8} {'p99':>8} {'sd':>8}  "
        f"{'p50':>8} {'p95':>8} {'p99':>8} {'sd':>8}  "
        f"{'(R/N)':>7}"
    )
    print(hdr1)
    print(hdr2)
    print(f"{'-'*TABLE_WIDTH}")

    for prefix, expected, label in SCALE_POINTS:
        try:
            actual = len(bck.list_all_objects(prefix=prefix))
            assert (
                actual == expected
            ), f"{label}: expected {expected} objects under '{prefix}', got {actual}"

            inv_name, create_sec = _create_inventory(client, bck, prefix)

            r_lats = [_list_ais(bck, prefix) for _ in range(runs)]
            n_lats = [_list_ais(bck, prefix, inv_name) for _ in range(runs)]
            s_lats = [_list_s3(s3, bucket, prefix) for _ in range(runs)]

            rp50, rp95, rp99, rsd = _pcts(r_lats)
            np50, np95, np99, nsd = _pcts(n_lats)
            sp50, sp95, sp99, ssd = _pcts(s_lats)
            speedup = rp50 / np50 if np50 > 0 else float("inf")

            print(
                f"{label:<9} {_ms(create_sec):>9}ms  "
                f"{_ms(rp50):>8} {_ms(rp95):>8} {_ms(rp99):>8} {_ms(rsd):>8}  "
                f"{_ms(np50):>8} {_ms(np95):>8} {_ms(np99):>8} {_ms(nsd):>8}  "
                f"{_ms(sp50):>8} {_ms(sp95):>8} {_ms(sp99):>8} {_ms(ssd):>8}  "
                f"{speedup:>6.1f}x"
            )
            bck.destroy_inventory(name=inv_name)

        except Exception as exc:  # pylint: disable=broad-exception-caught
            print(f"{label:<9} ERROR: {exc}")
            try:
                bck.destroy_inventory()
            except Exception:  # pylint: disable=broad-exception-caught
                pass

    print(f"{'='*TABLE_WIDTH}\n")


# ── Main ──────────────────────────────────────────────────────────────────────


def main() -> None:
    """Parse arguments and run the benchmark."""
    ap = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    ap.add_argument(
        "--endpoint",
        default=os.getenv("AIS_ENDPOINT", "http://localhost:8080"),
        help="AIS proxy URL (default: $AIS_ENDPOINT)",
    )
    ap.add_argument("--bucket", default="my-bucket")
    ap.add_argument("--provider", default="aws")
    ap.add_argument(
        "--runs",
        type=int,
        default=20,
        help="listing repetitions per scale point (default: 20)",
    )
    args = ap.parse_args()

    client = Client(args.endpoint)
    bck = client.bucket(args.bucket, provider=args.provider)
    s3 = boto3.client("s3")

    print(f"Endpoint : {args.endpoint}")
    print(f"Bucket   : {args.provider}://{args.bucket}")
    print(f"Runs     : {args.runs}")

    run(client, bck, s3, args.bucket, args.runs)


if __name__ == "__main__":
    main()
