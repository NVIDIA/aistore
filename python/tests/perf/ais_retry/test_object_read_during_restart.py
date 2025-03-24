"""
AIStore Multi-Threaded Object Reader

This script reads objects from an AIStore bucket using multiple threads, primarily to test the
retry logic in the AIStore client library. It also simulates object reads during AIStore cluster
restarts, ensuring resilience under dynamic conditions.
"""

import os
import logging
import random
import threading
import signal
import sys
import time
from dataclasses import dataclass
from typing import List, Dict
from aistore import Client

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(threadName)s - %(message)s",
    level=logging.INFO,  # Change to DEBUG for more detailed output
    handlers=[logging.FileHandler("aistore_read.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)


# pylint: disable=too-many-instance-attributes
@dataclass
class Config:
    """Configuration parameters for the AIStore reader.

    Attributes:
        endpoint: AIStore endpoint URL
        bucket_name: Name of the bucket to read from
        provider: Cloud provider for the bucket (e.g., 's3')
        num_threads: Number of worker threads to create
        min_delay: Minimum delay between object reads in seconds
        max_delay: Maximum delay between object reads in seconds
        timeout: Connection and read timeout tuple (connect, read)
        status_interval: Interval for logging statistics in seconds
    """

    endpoint: str = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
    bucket_name: str = os.environ.get("AIS_BUCKET", "resil-test")
    provider: str = os.environ.get("AIS_PROVIDER", "ais")
    num_threads: int = int(os.environ.get("THREAD_COUNT", "5"))
    min_delay: float = float(os.environ.get("MIN_DELAY", "0.1"))
    max_delay: float = float(os.environ.get("MAX_DELAY", "0.5"))
    timeout: tuple = (
        int(os.environ.get("CONNECT_TIMEOUT", "1")),
        int(os.environ.get("READ_TIMEOUT", "2")),
    )
    status_interval: int = int(os.environ.get("STATUS_INTERVAL", "5"))


class AISReader:
    """AIStore client wrapper for bucket operations."""

    def __init__(self, cluster_config: Config):
        """Initialize the AIStore reader with configuration.

        Args:
            cluster_config: Configuration parameters for the reader
        """
        self.config = cluster_config
        self.client = Client(cluster_config.endpoint, timeout=cluster_config.timeout)

    def list_objects(self) -> List[str]:
        """List all objects in the configured bucket.

        Returns:
            List of object names in the bucket

        Raises:
            ConnectionError: If unable to connect to AIStore cluster
            TimeoutError: If request times out
        """
        objects = self.client.bucket(
            bck_name=self.config.bucket_name, provider=self.config.provider
        ).list_objects()
        return [obj.name for obj in objects.entries]

    def read_object(self, object_name: str):
        """Read and discard content of an object WITHOUT error handling.

        Args:
            object_name: Name of the object to read

        Raises:
            Exception: For any errors during object read operation
        """
        logger.debug("Starting read: %s", object_name)
        obj_reader = (
            self.client.bucket(
                bck_name=self.config.bucket_name, provider=self.config.provider
            )
            .object(object_name)
            .get_reader()
        )
        obj_reader.read_all()
        logger.debug("Finished read: %s", object_name)


# pylint: disable=redefined-outer-name
def worker(
    config: Config,
    object_names: List[str],
    stats: Dict[str, int],
    stats_lock: threading.Lock,
    shutdown_event: threading.Event,
):
    """Worker thread function for continuous object reading.

    Args:
        config: Reader configuration
        object_names: List of available object names
        stats: Shared dictionary for tracking read counts
        stats_lock: Lock for thread-safe stats updates
        shutdown_event: Event for signaling thread shutdown
    """
    reader = AISReader(config)
    thread_name = threading.current_thread().name

    # Initialize thread stats once
    with stats_lock:
        stats[thread_name] = 0

    try:
        while not shutdown_event.is_set():
            obj_name = random.choice(object_names)
            try:
                reader.read_object(obj_name)
                # Update read count
                with stats_lock:
                    stats[thread_name] += 1
            except Exception as e:
                logger.error("Critical error reading %s: %s", obj_name, str(e))
                raise  # Re-raise to exit thread
            time.sleep(random.uniform(config.min_delay, config.max_delay))
    except Exception as e:
        logger.error("Thread %s terminating due to error: %s", thread_name, str(e))
    finally:
        # Clean up thread stats
        with stats_lock:
            del stats[thread_name]


if __name__ == "__main__":
    # Initialize configuration
    config = Config()

    # Initialize reader and get object list
    reader = AISReader(config)
    object_names = reader.list_objects()
    if not object_names:
        logger.error("No objects found in bucket")
        sys.exit(1)

    # Shared statistics tracking
    stats: Dict[str, int] = {}
    stats_lock = threading.Lock()

    # Create and start worker threads
    threads = []
    shutdown_event = threading.Event()

    # Signal handling for clean shutdown
    def handle_signal(_, __):
        """Signal handler for clean shutdown."""
        shutdown_event.set()
        logger.info("Shutdown signal received")

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    for _ in range(config.num_threads):
        thread = threading.Thread(
            target=worker,
            args=(config, object_names, stats, stats_lock, shutdown_event),
        )
        thread.start()
        threads.append(thread)

    # Monitoring loop
    try:
        while not shutdown_event.is_set():
            # Get thread statuses
            active = sum(1 for t in threads if t.is_alive())
            terminated = len(threads) - active

            # Get read statistics
            with stats_lock:
                stats_snapshot = stats.copy()

            # Log status report
            status_report = [f"Active Threads: {active} | Terminated: {terminated}"]
            status_report.append(
                " | ".join(
                    f"{thread}: {count} objs"
                    for thread, count in stats_snapshot.items()
                )
            )
            logger.info(" | ".join(status_report))
            time.sleep(config.status_interval)

    except Exception as e:
        logger.error("Fatal error: %s", str(e))
        shutdown_event.set()
    finally:
        for thread in threads:
            thread.join()
