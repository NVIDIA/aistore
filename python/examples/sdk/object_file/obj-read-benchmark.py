#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# This script benchmarks ObjectFile vs ObjectReader, comparing their performance 
# without interruptions.

import logging
import os
import shutil
import time
from pathlib import Path
import urllib3
from aistore.sdk.client import Client
from utils import obj_file_read, generate_data

logging.basicConfig(level=logging.DEBUG)

# Constants
KB = 1024
MB = 1024 * KB
GB = 1024 * MB

ENDPOINT = os.getenv("AIS_ENDPOINT", "http://localhost:51080")  # AIStore endpoint to communicate with
BUCKET_NAME = "benchmark-bucket"  # Name of the AIStore bucket
GENERATED_DIR = Path("gen-data")  # Directory to generate data
OUT_DIR = Path("output")  # Directory to store outputs
NUM_TARS = 10  # Number of tar archives to generate
NUM_FILES = 10  # Number of files in each tar archive
FILE_SIZE = 1 * GB  # Size of each file in the tar archive
READ_SIZES = [-1, 16 * KB, 32 * KB, 64 * KB, 128 * KB]  # Read sizes for benchmarking

def test_obj_reader_read_all(bucket, out_dir: Path):
    """
    Test reading data from the bucket using ObjectReader's read_all method.

    Args:
        bucket (Bucket): The AIStore bucket to read data from.
        out_dir (Path): Directory to store output files.
    """
    outputs = []
    start_time = time.time()
    out_dir.mkdir(parents=True, exist_ok=True)

    for entry in bucket.list_objects_iter():
        logging.info(f"Starting to read object using ObjectReader: {entry.name}")
        outfile_name = out_dir.joinpath(entry.name)
        outputs.append(outfile_name)

        with open(outfile_name, 'wb') as outfile:
            content = entry.object.get().read_all()  # ObjectReader read_all
            outfile.write(content)

    elapsed_time = time.time() - start_time
    logging.info(f"Completed ObjectReader read_all in {elapsed_time:.2f} seconds")
    return outputs

def cleanup_output_directory():
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    """
    Main function to benchmark ObjectFile vs ObjectReader.
    """
    # Initialize AIStore client (with retry on 400 and 404 errors)
    retry = urllib3.Retry(total=10, backoff_factor=0.5, status_forcelist=[400, 404])
    client = Client(endpoint=ENDPOINT, retry=retry)

    # Create and populate the bucket with generated data
    bucket = client.bucket(BUCKET_NAME).create()
    generate_data(bucket=bucket, num_tars=NUM_TARS, num_files=NUM_FILES, file_size=FILE_SIZE, dest=GENERATED_DIR)

    try:
        # ObjectFile: Test all read sizes, including read_all (-1)
        for read_size in READ_SIZES:
            logging.info(f"Running ObjectFile test with read_size = {read_size} bytes...")
            obj_file_read(bucket, read_size=read_size, out_dir=OUT_DIR)
            cleanup_output_directory()
        
        # ObjectReader: Read All
        logging.info("Running ObjectReader read_all test...")
        test_obj_reader_read_all(bucket, out_dir=OUT_DIR)
        cleanup_output_directory()

    finally:
        # Cleanup any leftover data
        shutil.rmtree(GENERATED_DIR, ignore_errors=True)
        shutil.rmtree(OUT_DIR, ignore_errors=True)
        bucket.delete(missing_ok=True)
        logging.info("Cleanup completed.")

if __name__ == "__main__":
    main()
