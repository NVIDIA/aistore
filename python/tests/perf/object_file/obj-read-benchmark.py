#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import logging
import os
import time
import csv
import shutil
import tarfile
from pathlib import Path
from typing import List
from aistore.sdk.client import Client
from aistore.sdk.obj.object import Object
from utils import calculate_median_throughput, clear_directory, generate_and_upload_tar

logging.basicConfig(level=logging.DEBUG)

KB = 1024
MB = 1024 * KB
GB = 1024 * MB

AIS_ENDPOINT = os.getenv("AIS_ENDPOINT", "http://localhost:51080")
BUCKET_NAME = "benchmark-bucket"
TAR_OBJECT_NAME = "benchmark-tar"
GENERATE_PATH = Path("generated_data")
EXTRACT_PATH = Path("extracted_data")

# Define file size and count for the benchmark tar file
FILE_SIZE = 10 * MB
NUM_FILES = 100
TOTAL_SIZE = FILE_SIZE * NUM_FILES

# Define the number of reads for the benchmark (to calculate median throughput)
NUM_READS = 200


def benchmark_obj_reader(obj: Object) -> List[float]:
    times = []
    for _ in range(NUM_READS):
        start_time = time.perf_counter()
        with tarfile.open(fileobj=obj.get_reader().raw(), mode="r|*") as tar:
            tar.extractall(path=EXTRACT_PATH)
        times.append(time.perf_counter() - start_time)
        clear_directory(EXTRACT_PATH)
    return times


def benchmark_obj_file_reader(obj: Object) -> List[float]:
    times = []
    for _ in range(NUM_READS):
        start_time = time.perf_counter()
        with obj.get_reader().as_file() as obj_file:
            with tarfile.open(fileobj=obj_file, mode="r|*") as tar:
                tar.extractall(path=EXTRACT_PATH)
        times.append(time.perf_counter() - start_time)
        clear_directory(EXTRACT_PATH)
    return times


def main():
    client = Client(endpoint=AIS_ENDPOINT)
    bucket = client.bucket(BUCKET_NAME).create()
    obj = bucket.object(TAR_OBJECT_NAME)

    try:
        GENERATE_PATH.mkdir(parents=True, exist_ok=True)
        EXTRACT_PATH.mkdir(parents=True, exist_ok=True)

        # Generate and upload tar file
        generate_and_upload_tar(obj, NUM_FILES, FILE_SIZE, GENERATE_PATH)

        # Benchmark with ObjectReader NUM_READS times and calculate median throughput
        reader_times = benchmark_obj_reader(obj)
        reader_throughput = calculate_median_throughput(reader_times, TOTAL_SIZE) / MB

        # Benchmark with ObjectFileReader NUM_READS times and calculate median throughput
        file_reader_times = benchmark_obj_file_reader(obj)
        file_reader_throughput = (
            calculate_median_throughput(file_reader_times, TOTAL_SIZE) / MB
        )

        # Calculate the median % overhead
        overhead = (
            (reader_throughput - file_reader_throughput) / reader_throughput
        ) * 100

        # Save results to CSV
        with open("benchmark_results.csv", mode="w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(
                [
                    "File Size (MB)",
                    "File Count",
                    "ObjectReader Throughput (MB/s)",
                    "ObjectFileReader Throughput (MB/s)",
                    "Overhead (%)",
                ]
            )
            writer.writerow(
                [
                    FILE_SIZE / MB,
                    NUM_FILES,
                    reader_throughput,
                    file_reader_throughput,
                    overhead,
                ]
            )

    finally:
        bucket.delete()
        shutil.rmtree(GENERATE_PATH, ignore_errors=True)
        shutil.rmtree(EXTRACT_PATH, ignore_errors=True)


if __name__ == "__main__":
    main()
