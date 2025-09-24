"""
Comprehensive benchmarking tool for AIStore batch operations.

Provides functionality to benchmark sequential vs batch object retrieval operations,
with detailed performance analysis and comparison reporting.
"""

import time
import json
from dataclasses import dataclass, asdict
from typing import List
from aistore import Client
from aistore.sdk.batch.batch_request import BatchRequest


@dataclass
class BenchmarkResult:
    """Structured benchmark result data"""

    bucket_name: str
    test_type: str  # 'sequential' or 'batch'
    streaming: bool = None  # None for sequential, True/False for batch
    batch_size: int = None  # None for sequential, actual batch size for batch
    num_batches: int = None  # None for sequential, number of batches for batch
    total_objects: int = 0
    duration: float = 0.0
    total_bytes: int = 0
    successful_reads: int = 0
    failed_reads: int = 0
    throughput_objects_per_sec: float = 0.0
    throughput_mib_per_sec: float = 0.0

    def calculate_throughput(self):
        """Calculate throughput metrics"""
        if self.duration > 0:
            self.throughput_objects_per_sec = self.successful_reads / self.duration
            self.throughput_mib_per_sec = (
                self.total_bytes / (1024 * 1024)
            ) / self.duration


@dataclass
class BenchmarkComparison:
    """Comparison between benchmark results"""

    test_config: str  # Description of test configuration
    result: BenchmarkResult
    baseline: BenchmarkResult = None
    time_difference: float = 0.0  # Positive means slower than baseline
    time_percentage: float = 0.0  # Percentage difference from baseline
    speedup_factor: float = 0.0  # How many times faster/slower
    throughput_improvement: float = 0.0  # Throughput ratio vs baseline

    def compare_to_baseline(self, baseline: BenchmarkResult):
        """Compare this result to a baseline"""
        self.baseline = baseline
        if baseline and baseline.duration > 0:
            self.time_difference = self.result.duration - baseline.duration
            self.time_percentage = (self.time_difference / baseline.duration) * 100
            self.speedup_factor = (
                baseline.duration / self.result.duration
                if self.result.duration > 0
                else 0
            )
            self.throughput_improvement = (
                (
                    self.result.throughput_objects_per_sec
                    / baseline.throughput_objects_per_sec
                )
                if baseline.throughput_objects_per_sec > 0
                else 0
            )


def benchmark_sequential_get(
    client: Client, bucket_name: str, num_objects: int
) -> BenchmarkResult:
    """Benchmark getting objects one by one sequentially."""
    print(f"Starting sequential read of {num_objects} objects...")

    bucket = client.bucket(bucket_name)
    total_bytes = 0
    successful_reads = 0
    failed_reads = 0

    start_time = time.time()

    for i in range(num_objects):
        obj_name = f"benchmark_obj_{i:06d}.txt"
        try:
            obj = bucket.object(obj_name)
            data = obj.get_reader().as_file().read()
            total_bytes += len(data)
            successful_reads += 1
        except Exception as e:
            print(f"Failed to read {obj_name}: {e}")
            failed_reads += 1

    end_time = time.time()
    duration = end_time - start_time

    print(
        f"Sequential read completed: {successful_reads} successful, {failed_reads} failed"
    )

    # Create result object
    result = BenchmarkResult(
        bucket_name=bucket_name,
        test_type="sequential",
        streaming=None,
        batch_size=None,
        num_batches=None,
        total_objects=num_objects,
        duration=duration,
        total_bytes=total_bytes,
        successful_reads=successful_reads,
        failed_reads=failed_reads,
    )
    result.calculate_throughput()
    return result


def benchmark_get_batch(
    client: Client,
    bucket_name: str,
    num_objects: int,
    batch_size: int = 1000,
    streaming: bool = True,
) -> BenchmarkResult:
    """Benchmark getting objects using batch API in chunks."""
    # print(f"Starting batch read of {num_objects} objects in chunks of {batch_size}, streaming={streaming}...")

    bucket = client.bucket(bucket_name)
    batch_loader = client.batch_loader()
    total_bytes = 0
    successful_reads = 0
    failed_reads = 0
    num_batches = (
        num_objects + batch_size - 1
    ) // batch_size  # Calculate number of batches

    start_time = time.time()

    # Process in batches
    for batch_start in range(0, num_objects, batch_size):
        batch_end = min(batch_start + batch_size, num_objects)
        current_batch_size = batch_end - batch_start

        # print(f"Processing batch {batch_start//batch_size + 1}: objects {batch_start} to {batch_end-1}")

        # Create batch request for current chunk
        batch_req = BatchRequest(
            output_format=".tar",
            continue_on_err=True,
            only_obj_name=False,
            streaming=streaming,
        )

        # Add objects in current batch
        for i in range(batch_start, batch_end):
            obj_name = f"benchmark_obj_{i:06d}.txt"
            obj = bucket.object(obj_name)
            batch_req.add_object_request(obj=obj)

        try:
            # Execute batch request
            batch = batch_loader.get_batch(batch_req)

            # Process results
            batch_successful = 0
            for _, data in batch:
                total_bytes += len(data)
                batch_successful += 1

            successful_reads += batch_successful
            batch_failed = current_batch_size - batch_successful
            failed_reads += batch_failed

            # print(f"Batch completed: {batch_successful} successful, {batch_failed} failed")

        except Exception as e:
            print(f"Batch {batch_start//batch_size + 1} failed: {e}")
            failed_reads += current_batch_size

    end_time = time.time()
    duration = end_time - start_time

    print(f"Batch read completed: {successful_reads} successful, {failed_reads} failed")

    # Create result object
    result = BenchmarkResult(
        bucket_name=bucket_name,
        test_type="batch",
        streaming=streaming,
        batch_size=batch_size,
        num_batches=num_batches,
        total_objects=num_objects,
        duration=duration,
        total_bytes=total_bytes,
        successful_reads=successful_reads,
        failed_reads=failed_reads,
    )
    result.calculate_throughput()
    return result


def run_comprehensive_benchmark(
    client: Client, bucket_name: str, total_objects: int = 100000
) -> List[BenchmarkComparison]:
    """Run comprehensive benchmark with all requested combinations"""
    results = []

    print(f"\n{'='*100}")
    print("COMPREHENSIVE BATCH BENCHMARK")
    print(f"{'='*100}")
    print(f"Bucket: {bucket_name}")
    print(f"Total objects per test: {total_objects:,}")

    # Test configurations as requested
    test_configs = [
        # Batch configurations: (batch_size, num_batches, streaming)
        (100, 100, True),  # 100 objects per batch * 100 batches, streaming=True
        (100, 100, False),  # 100 objects per batch * 100 batches, streaming=False
        (1000, 10, True),  # 1,000 objects per batch * 10 batches, streaming=True
        (1000, 10, False),  # 1,000 objects per batch * 10 batches, streaming=False
        (10000, 1, True),  # 10,000 objects per batch * 1 batch, streaming=True
        (10000, 1, False),  # 10,000 objects per batch * 1 batch, streaming=False
    ]

    # First run sequential benchmark as baseline
    print(f"\n{'*'*60}")
    print("RUNNING BASELINE: Sequential Read")
    print(f"{'*'*60}")
    baseline_result = benchmark_sequential_get(client, bucket_name, total_objects)
    print(f"Baseline completed in {baseline_result.duration:.2f} seconds")

    # Run batch benchmarks
    for batch_size, num_batches, streaming in test_configs:
        print(f"\n{'*'*60}")
        test_config = f"Batch {batch_size}x{num_batches} (streaming={streaming})"
        print(f"RUNNING: {test_config}")
        print(f"{'*'*60}")

        try:
            batch_result = benchmark_get_batch(
                client, bucket_name, total_objects, batch_size, streaming
            )

            # Create comparison
            comparison = BenchmarkComparison(
                test_config=test_config, result=batch_result
            )
            comparison.compare_to_baseline(baseline_result)
            results.append(comparison)

            print(f"Test completed in {batch_result.duration:.2f} seconds")

        except Exception as e:
            print(f"ERROR in {test_config}: {e}")
            continue

        # Add small delay between tests
        print("Waiting 3 seconds before next test...")
        time.sleep(3)

    return results


def print_comprehensive_results(results: List[BenchmarkComparison]):
    """Print comprehensive results with baseline comparisons"""

    print(f"\n{'='*120}")
    print("COMPREHENSIVE BENCHMARK RESULTS")
    print(f"{'='*120}")

    # Print baseline info
    if results and results[0].baseline:
        baseline = results[0].baseline
        print("\nBASELINE (Sequential Read):")
        print(f"  Bucket:           {baseline.bucket_name}")
        print(f"  Objects:          {baseline.total_objects:,}")
        print(f"  Duration:         {baseline.duration:.2f} seconds")
        print(
            f"  Throughput:       {baseline.throughput_objects_per_sec:.1f} objects/s"
        )
        print(f"  Data Throughput:  {baseline.throughput_mib_per_sec:.2f} MiB/s")

    print(f"\n{'='*120}")
    print(
        f"{'Test Configuration':<35} {'Duration':<10} {'vs Baseline':<20} {'Speedup':<10} {'Throughput':<15} {'vs Baseline':<12}"
    )
    print(f"{'='*120}")

    for comp in results:
        result = comp.result

        # Format duration difference
        time_diff_str = f"{comp.time_difference:+.2f}s" if comp.baseline else "N/A"
        percentage_str = f"({comp.time_percentage:+.1f}%)" if comp.baseline else ""
        time_vs_baseline = f"{time_diff_str} {percentage_str}"[:20]

        # Format speedup
        if comp.speedup_factor > 1:
            speedup_str = f"{comp.speedup_factor:.2f}x"
        elif comp.speedup_factor > 0:
            speedup_str = f"{1/comp.speedup_factor:.2f}x slower"
        else:
            speedup_str = "N/A"

        # Format throughput improvement
        throughput_str = f"{result.throughput_objects_per_sec:.1f}/s"
        if comp.baseline:
            improvement_str = f"{comp.throughput_improvement:.2f}x"
        else:
            improvement_str = "N/A"

        print(
            f"{comp.test_config:<35} {result.duration:<10.2f} {time_vs_baseline:<20} {speedup_str:<10} {throughput_str:<15} {improvement_str:<12}"
        )

    print(f"{'='*120}")

    # Detailed results section
    print("\nDETAILED RESULTS:")
    print(f"{'='*120}")

    for i, comp in enumerate(results, 1):
        result = comp.result
        print(f"\n{i}. {comp.test_config}")
        print(f"   Bucket:           {result.bucket_name}")
        print(f"   Batch Size:       {result.batch_size:,} objects")
        print(f"   Number of Batches: {result.num_batches}")
        print(f"   Streaming:        {result.streaming}")
        print(f"   Total Objects:    {result.total_objects:,}")
        print(f"   Duration:         {result.duration:.3f} seconds")
        print(
            f"   Success Rate:     {result.successful_reads}/{result.total_objects} ({100*result.successful_reads/result.total_objects:.1f}%)"
        )
        print(f"   Throughput:       {result.throughput_objects_per_sec:.1f} objects/s")
        print(f"   Data Throughput:  {result.throughput_mib_per_sec:.2f} MiB/s")

        if comp.baseline:
            print("   vs Baseline:")
            print(
                f"     Time Difference:  {comp.time_difference:+.3f} seconds ({comp.time_percentage:+.1f}%)"
            )
            print(
                f"     Speed Factor:     {comp.speedup_factor:.2f}x {'faster' if comp.speedup_factor > 1 else 'slower'}"
            )
            print(f"     Throughput Ratio: {comp.throughput_improvement:.2f}x")


def save_results_to_json(results: List[BenchmarkComparison], filename: str = None):
    """Save benchmark results to JSON file"""
    if not filename:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = f"benchmark_results_{timestamp}.json"

    # Convert to serializable format
    data = {
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "baseline": (
            asdict(results[0].baseline) if results and results[0].baseline else None
        ),
        "results": [],
    }

    for comp in results:
        result_data = {
            "test_config": comp.test_config,
            "result": asdict(comp.result),
            "time_difference": comp.time_difference,
            "time_percentage": comp.time_percentage,
            "speedup_factor": comp.speedup_factor,
            "throughput_improvement": comp.throughput_improvement,
        }
        data["results"].append(result_data)

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"\nResults saved to: {filename}")
    return filename


def main():
    """Main comprehensive benchmarking function."""
    # Configuration
    AISTORE_URL = "http://<ais-endpt>:51080"  # Replace <ais-endpt> with the actual AIStore endpoint
    BUCKET_NAME = "benchmark-10KiB"  # Updated to match populate_bucket.py
    TOTAL_OBJECTS = 10000  # 100,000 objects as requested

    print("Starting comprehensive batch benchmark...")
    print(f"AIStore URL: {AISTORE_URL}")
    print(f"Bucket: {BUCKET_NAME}")
    print(f"Total objects per test: {TOTAL_OBJECTS:,}")

    # Create client with optimized pool size
    client = Client(AISTORE_URL, max_pool_size=1000)

    # Verify bucket exists
    try:
        bucket = client.bucket(BUCKET_NAME)
        bucket.head()
        print(f"Bucket '{BUCKET_NAME}' found - proceeding with benchmark")
    except Exception as e:
        print(f"Error accessing bucket '{BUCKET_NAME}': {e}")
        print("Make sure to run populate_bucket.py first to create test objects")
        print("The bucket should contain at least 100,000 objects for this benchmark")
        return

    # Run comprehensive benchmark
    try:
        results = run_comprehensive_benchmark(client, BUCKET_NAME, TOTAL_OBJECTS)

        if not results:
            print("No benchmark results obtained. Check for errors above.")
            return

        # Display comprehensive results
        print_comprehensive_results(results)

        # Save results to JSON file
        json_file = save_results_to_json(results)

        print(f"\n{'='*100}")
        print("BENCHMARK COMPLETE!")
        print(f"{'='*100}")
        print(f"Total tests run: {len(results)}")
        print(f"Results saved to: {json_file}")
        print(f"{'='*100}")

    except KeyboardInterrupt:
        print("\n\nBenchmark interrupted by user")
    except Exception as e:
        print(f"\nBenchmark failed with error: {e}")


if __name__ == "__main__":
    main()
