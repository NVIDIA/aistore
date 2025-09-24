"""
Populate bucket with objects using multi-threading

This script provides a utility class and logic to populate an AIStore bucket with a large number of objects,
primarily for benchmarking and testing purposes.

Classes:
    BucketPopulator: Handles concurrent uploading of objects to a specified AIStore bucket, with retry logic,
    progress reporting, and statistics tracking.

Usage:
    - Instantiate a BucketPopulator with an aistore.Client instance.
    - Use the upload_object or upload_batch methods to populate a bucket with test objects.
"""

import time
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from tqdm import tqdm
from aistore import Client


class BucketPopulator:
    """Handles concurrent uploading of objects to AIStore buckets with retry logic and statistics tracking."""

    def __init__(
        self, aistore_client: Client, max_workers: int = 32, chunk_size: int = 100
    ):
        self.client = aistore_client
        self.max_workers = max_workers
        self.chunk_size = chunk_size
        self.lock = Lock()
        self.stats = {"success": 0, "failed": 0, "retried": 0}

        # Setup logging
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def upload_object(self, bucket, obj_name: str, content: bytes, retries: int = 3):
        """Upload a single object with retry logic"""
        for attempt in range(retries):
            try:
                obj = bucket.object(obj_name)
                obj.get_writer().put_content(content)

                with self.lock:
                    self.stats["success"] += 1
                    if attempt > 0:
                        self.stats["retried"] += 1
                return True

            except (ConnectionError, TimeoutError, OSError) as e:
                if attempt < retries - 1:
                    time.sleep(0.1 * (2**attempt))  # Exponential backoff
                    continue
                else:
                    with self.lock:
                        self.stats["failed"] += 1
                    self.logger.error(
                        "Failed to upload %s after %d attempts: %s",
                        obj_name,
                        retries,
                        e,
                    )
                    return False
        return False

    def upload_batch(self, bucket, start_idx: int, batch_size: int, content: bytes):
        """Upload a batch of objects"""
        results = []
        for i in range(start_idx, min(start_idx + batch_size, start_idx + batch_size)):
            obj_name = f"benchmark_obj_{i:06d}.txt"
            success = self.upload_object(bucket, obj_name, content)
            results.append(success)
        return results

    def populate_bucket(
        self, bucket_name: str, num_objects: int, object_size: int = 1024
    ):
        """Populate bucket with objects using multi-threading"""
        self.logger.info(
            "Starting population of bucket '%s' with %d objects",
            bucket_name,
            num_objects,
        )
        self.logger.info(
            "Using %d threads, object size: %d bytes", self.max_workers, object_size
        )

        start_time = time.time()

        # Create bucket and content
        bucket = self.client.bucket(bucket_name).create(exist_ok=True)
        content = b"x" * int(object_size)

        # Reset stats
        self.stats = {"success": 0, "failed": 0, "retried": 0}

        # Create progress bar
        with tqdm(total=num_objects, desc="Uploading objects", unit="objects") as pbar:
            # Submit work to thread pool
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit individual upload tasks
                futures = []
                for i in range(num_objects):
                    obj_name = f"benchmark_obj_{i:06d}.txt"
                    future = executor.submit(
                        self.upload_object, bucket, obj_name, content
                    )
                    futures.append(future)

                # Process completed uploads
                for future in as_completed(futures):
                    future.result()  # This will also raise any exceptions
                    pbar.update(1)

        end_time = time.time()
        duration = end_time - start_time

        # Print statistics
        self.print_stats(duration, num_objects, object_size)

    def print_stats(self, duration: float, num_objects: int, object_size: int):
        """Print performance statistics"""
        total_data_mb = (num_objects * object_size) / (1024 * 1024)
        throughput_objects = num_objects / duration
        throughput_mb = total_data_mb / duration

        print("\n" + "=" * 60)
        print("POPULATION COMPLETE")
        print("=" * 60)
        print(f"Total objects:     {num_objects:,}")
        print(f"Successful:        {self.stats['success']:,}")
        print(f"Failed:            {self.stats['failed']:,}")
        print(f"Retried:           {self.stats['retried']:,}")
        print(f"Duration:          {duration:.2f} seconds")
        print(f"Throughput:        {throughput_objects:.1f} objects/second")
        print(f"Data throughput:   {throughput_mb:.2f} MB/second")
        print(f"Total data:        {total_data_mb:.2f} MB")
        print("=" * 60)


def populate_bucket(
    aistore_client: Client,
    bucket_name: str,
    num_objects: int,
    object_size: int = 1024,
    max_workers: int = 32,
):
    """Convenience function for backward compatibility"""
    bucket_populator = BucketPopulator(aistore_client, max_workers=max_workers)
    bucket_populator.populate_bucket(bucket_name, num_objects, object_size)


if __name__ == "__main__":
    client = Client("http://<ais-lb>:51080", max_pool_size=1000)

    # Configuration - adjust these parameters for optimal performance
    BUCKET_NAME = "benchmark-100KiB"
    NUM_OBJECTS = 100000
    OBJECT_SIZE = 100 * 1024
    MAX_WORKERS = 120

    populator = BucketPopulator(client, max_workers=MAX_WORKERS)
    populator.populate_bucket(BUCKET_NAME, NUM_OBJECTS, OBJECT_SIZE)
