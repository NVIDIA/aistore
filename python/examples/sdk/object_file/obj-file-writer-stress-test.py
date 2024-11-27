#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# This script tests AIStore's ObjectFileWriter and its ability to handle interruptions 
# during a write workload (e.g., simulating intermittent AIStore K8s node failures). Run with
# a one-node cluster to best and most consistently observe the behavior of the ObjectFileWriter.

import logging
import os
import time
import urllib3
import random
import string
from kubernetes import client as k8s_client, config as k8s_config
from aistore.sdk.client import Client
from utils import start_pod_killer, stop_pod_killer

logging.basicConfig(level=logging.INFO)  # Set to DEBUG for more detailed logs

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

# Adjust the write size, pod kill interval, and write count to control the workload
# and frequency of interruptions based on your specific test machine configuration.
AIS_ENDPOINT = os.getenv("AIS_ENDPOINT", "http://localhost:51080")
BUCKET_NAME = "writer-stress-test"
OBJECT_NAME = "writer-stress-test-object"
WRITE_COUNT = 1000
WRITE_SIZE = 128 * KB
INTERRUPT = True
POD_KILL_NAMESPACE = "ais"
POD_KILL_NAME = "ais-target-0"
POD_KILL_INTERVAL = 1e-3


def generate_random_data(size: int) -> bytes:
    """Generate random data of the specified size."""
    return "".join(random.choices(string.ascii_letters + string.digits, k=size)).encode()


def test_with_interruptions(k8s_client: k8s_client.CoreV1Api, obj):
    """Test ObjectFileWriter with pod interruptions."""
    logging.info("Starting ObjectFileWriter stress test")

    # Start pod killer process
    pod_killer_process = None
    if INTERRUPT:
        logging.info("Starting pod killer process...")
        pod_killer_process = start_pod_killer(k8s_client, POD_KILL_NAMESPACE, POD_KILL_NAME, POD_KILL_INTERVAL)

    # Perform write workload
    expected_data = obj_file_write(obj)

    # Stop the pod killer process
    if INTERRUPT:
        logging.info("Stopping pod killer process...")
        stop_pod_killer(pod_killer_process)

    # Validate written data after interruptions
    logging.info("Validating written content...")
    actual_data = obj.get().read_all()
    assert actual_data == expected_data, "Validation Failed: Written content does not match expected content"
    logging.info("Validation Passed: Written content matches expected content.")


def obj_file_write(obj):
    """Perform the write workload using ObjectFileWriter and return expected data."""
    writer = obj.put().as_file(mode="a")
    expected_data = []  # Track all chunks written
    with writer:
        for i in range(WRITE_COUNT):
            chunk = generate_random_data(WRITE_SIZE)
            writer.write(chunk)
            expected_data.append(chunk)
            logging.info(f"Written chunk {i + 1}/{WRITE_COUNT}")
    return b"".join(expected_data)


def main():
    """Main function to execute the stress test."""
    retry = urllib3.Retry(total=10, backoff_factor=0.5, status_forcelist=[400, 404])
    client = Client(endpoint=AIS_ENDPOINT, retry=retry)
    k8s_config.load_kube_config()
    v1 = k8s_client.CoreV1Api()

    # Create bucket and object for testing
    bucket = client.bucket(BUCKET_NAME).create()
    obj = bucket.object(OBJECT_NAME)

    try:
        # Test writing to the object with pod interruptions
        test_with_interruptions(v1, obj)
    finally:
        # Cleanup bucket
        bucket.delete(missing_ok=True)
        logging.info("Cleanup completed.")


if __name__ == "__main__":
    main()
