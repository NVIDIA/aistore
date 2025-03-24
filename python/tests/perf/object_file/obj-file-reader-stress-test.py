#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# This script tests AIStore's ObjectFileReader and its ability to resume object reading with interruptions
# to the underlying object stream (e.g. simulating intermittent AIStore K8s target node failures). Run with
# a small multi-target cluster (e.g. two targets) to best and most consistently observe the behavior of the
# ObjectFileReader's resume functionality.

import logging
import os
import time
from kubernetes import client as k8s_client, config as k8s_config
from aistore.sdk.client import Client
from aistore.sdk.obj.object_reader import ObjectReader
from utils import (
    create_and_put_object,
    obj_file_reader_read,
    start_pod_killer,
    stop_pod_killer,
)

logging.basicConfig(level=logging.INFO)  # Set to DEBUG for more detailed logs

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

# Adjust the object size, pod kill interval, and chunk size to control how often interruptions
# occur based on your specific test machine configuration and network setup. For example, increase
# object size or decrease chunk size / pod kill interval to trigger more frequent disruptions.

AIS_ENDPOINT = os.getenv("AIS_ENDPOINT", "http://localhost:51080")
BUCKET_NAME = "stress-test"
OBJECT_NAME = "stress-test-file"
OBJECT_SIZE = 1 * GB
MAX_RESUME = 100  # Set to a high value to allow resumes for stress-test
INTERRUPT = True
POD_KILL_NAMESPACE = "ais"
POD_KILL_INTERVAL = 1e-3
CHUNK_SIZE = 128
READ_SIZE = -1


def test_with_interruptions(
    k8s_client: k8s_client.CoreV1Api, object_reader: ObjectReader, generated_data: bytes
):
    """Test ObjectFileReader read with pod interruptions and validate data."""
    logging.info("Starting test")

    start_time = time.time()

    # Start pod killer process
    if INTERRUPT:
        logging.info("Starting pod killer process...")
        pod_killer_process = start_pod_killer(
            k8s_client, POD_KILL_NAMESPACE, POD_KILL_INTERVAL
        )

    # Perform ObjectFileReader read
    downloaded_data, resume_total = obj_file_reader_read(
        object_reader, READ_SIZE, BUFFER_SIZE, MAX_RESUME
    )

    end_time = time.time()

    logging.info(
        f"Stress-test completed in {end_time - start_time:.2f} seconds w/ {resume_total} resumes"
    )

    # Stop the pod killer process
    if INTERRUPT:
        logging.info("Stopping pod killer process...")
        stop_pod_killer(pod_killer_process)

    # Validate the downloaded data by comparing it to the generated data
    assert (
        downloaded_data == generated_data
    ), "Validation Failed: Downloaded data does not match generated data"
    logging.info("Validation Passed: Downloaded data matches the generated data")


def main():
    """Main function to run the stress-test."""
    client = Client(endpoint=AIS_ENDPOINT)
    k8s_config.load_kube_config()
    v1 = k8s_client.CoreV1Api()
    bucket = client.bucket(BUCKET_NAME).create()
    obj = bucket.object(OBJECT_NAME)

    try:
        # Generate data and upload to the bucket
        logging.info("Generating data and uploading to the bucket...")
        generated_data = create_and_put_object(obj, OBJECT_SIZE)
        object_reader = obj.get_reader(chunk_size=CHUNK_SIZE)

        # Test reading the object with pod interruptions
        test_with_interruptions(v1, object_reader, generated_data)
    finally:
        bucket.delete(missing_ok=True)
        logging.info("Cleanup completed.")


if __name__ == "__main__":
    main()
