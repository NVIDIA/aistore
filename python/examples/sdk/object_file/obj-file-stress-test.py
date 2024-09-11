#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# This script tests AIStore's ObjectFile and its ability to resume object 
# reading with interruptions to the underlying object stream (e.g. 
# simulating intermittent AIStore K8s node failures).

import logging
import os
import shutil
import time
from pathlib import Path

import urllib3
from aistore.sdk.client import Client
from kubernetes import client as k8s_client, config as k8s_config

from utils import generate_data, start_pod_killer, stop_pod_killer, obj_file_read

logging.basicConfig(level=logging.DEBUG)

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

ENDPOINT = os.getenv("AIS_ENDPOINT", "http://localhost:51080")  # AIStore endpoint to communicate with
BUCKET_NAME = "objfile-test-data"  # Name of the AIStore bucket where tar files are stored
GENERATED_DIR = Path("gen-data")  # Directory to generate data
OUT_DIR = Path("output")  # Directory to store outputs
NUM_TARS = 1  # Number of tar archives to generate
NUM_FILES = 20  # Number of files in each tar archive
FILE_SIZE = 1 * GB  # Size of each file in the tar archive
NAMESPACE = "ais"  # Kubernetes namespace for the pod killer
POD_PREFIX = "ais"  # Prefix to identify pods to be killed during the test
POD_KILL_INTERVAL = (30, 60)  # Interval (in seconds) between pod killings
READ_SIZES = [-1, 16 * KB, 32 * KB, 64 * KB, 128 * KB]  # Read sizes to test


def validate(bucket, outputs):
    """
    Validate the downloaded objects by comparing to local files.

    Args:
        bucket (Bucket): The AIStore bucket for validation.
        outputs (list): List of local file paths to validate.
    """
    for output in outputs:
        logging.info(f"Validating object {output.parts[-1]}...")
        with open(output, 'rb') as result:
            result_bytes = result.read()
        ais_bytes = bucket.object(obj_name=output.parts[-1]).get().read_all()
        assert ais_bytes == result_bytes, f"Validation failed for {output.parts[-1]}"
    logging.info("All objects validated successfully.")
    shutil.rmtree(OUT_DIR, ignore_errors=True)


def test_with_interruptions(bucket, read_size):
    """
    Run the object file read test with pod-killing interruptions.

    Args:
        bucket (Bucket): The AIStore bucket to read from.
        read_size (int): Size of chunks to read in bytes.
    """
    logging.info(f"Starting test with interruptions for read size: {read_size} bytes")
    
    # Start the pod killer process
    stop_event, pod_killer_process = start_pod_killer(
        namespace=NAMESPACE, k8s_client=v1, pod_prefix=POD_PREFIX, pod_kill_interval=POD_KILL_INTERVAL
    )
    
    # Call the imported obj_file_read function from utils.py
    result = obj_file_read(bucket, read_size=read_size, out_dir=OUT_DIR)
    
    # Stop the pod killer process
    stop_pod_killer(stop_event=stop_event, pod_killer_process=pod_killer_process)
    
    time.sleep(20)  # Wait for any pods to settle
    
    # Validate the downloaded files
    validate(bucket, result)


def main():
    """
    Main function to run the ObjectFile read tests with interruptions.
    """
    # Initialize AIStore client (with retry on 400 and 404 errors)
    retry = urllib3.Retry(total=10, backoff_factor=0.5, status_forcelist=[400, 404])
    client = Client(endpoint=ENDPOINT, retry=retry)

    # Initialize Kubernetes client
    k8s_config.load_kube_config()
    global v1
    v1 = k8s_client.CoreV1Api()

    # Generate Data & Populate Bucket:
    bucket = client.bucket(BUCKET_NAME).create()
    generate_data(bucket=bucket, num_tars=NUM_TARS, num_files=NUM_FILES, file_size=FILE_SIZE, dest=GENERATED_DIR)

    try:
        # Test reading in various read sizes with pod interruptions
        for read_size in READ_SIZES:
            test_with_interruptions(bucket, read_size=read_size)
    finally:
        # Cleanup any leftover data
        shutil.rmtree(GENERATED_DIR, ignore_errors=True)
        shutil.rmtree(OUT_DIR, ignore_errors=True)
        bucket.delete(missing_ok=True)
        logging.info("Cleanup completed.")


if __name__ == "__main__":
    main()
