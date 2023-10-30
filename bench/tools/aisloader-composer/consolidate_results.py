"""
Python script designed to analyze and amalgamate the data from 
AIS Loader-generated files into a cohesive report

Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
"""

import os
import re
import sys

# Specify the folder where your files are located
if len(sys.argv) != 2:
    FOLDER_PATH = "/path/to/directory"
else:
    FOLDER_PATH = sys.argv[1]

MIN_LATENCY = float("inf")
MAX_LATENCY = 0
SUM_AVG_LATENCY = 0
SUM_THROUGHPUT = 0
SUM_ERRORS = 0
COUNT = 0

# Regular expression to match various time components in the input string
time_regex = re.compile(
    r"(?:(?P<hours>\d+)h)?"  # Capture hours
    r"(?:(?P<minutes>\d+)m)?"  # Capture minutes
    r"(?:(?P<seconds>\d+(?:\.\d*)?)s)?"  # Capture seconds
    r"(?:(?P<milliseconds>\d+(?:\.\d*)?)ms)?"  # Capture milliseconds
    r"(?:(?P<microseconds>\d+(?:\.\d*)?)(?:µs|us|μs))?"
)

# Conversion factors to milliseconds
time_units = {
    "hours": 3600000,
    "minutes": 60000,
    "seconds": 1000,
    "milliseconds": 1,
    "microseconds": 0.001,
    "microseconds_alt": 0.001,
}


def convert_to_ms(time_str):
    """
    Interprets the time string from the report and converts it to
    milliseconds (ms).
    """
    # Normalize the input string
    normalized_time_str = time_str.strip().replace("us", "μs")

    # Match the input string against the regex pattern
    match = time_regex.fullmatch(normalized_time_str)

    if not match:
        raise ValueError(f"Invalid time format: {time_str}")

    # Initialize total milliseconds
    total_ms = 0.0

    # Iterate through matched groups and calculate total milliseconds
    for unit, value in match.groupdict(default="").items():
        if value:
            total_ms += float(value) * time_units[unit]

    return total_ms


# Regular expression to match throughput strings
throughput_regex = re.compile(r"(\d+\.?\d*)\s*(\w+)")

# Conversion factors from various units to GiB/s
throughput_units = {
    "GiB": 1,
    "MiB": 1 / 1024,
    "KiB": 1 / (1024**2),
}


def convert_to_gib_per_second(throughput_str):
    """
    Transforms the throughput value reported as a string into a
    floating-point number representing gibibytes per second (GiB/s).
    """
    match = throughput_regex.match(throughput_str)
    if not match:
        raise ValueError(f"Invalid throughput format: {throughput_str}")

    # Extract number and unit from the match
    number, unit = match.groups()
    number = float(number)

    # Check if the unit is valid
    if unit not in throughput_units:
        raise ValueError(f"Invalid throughput unit: {unit}")

    # Perform the conversion
    return number * throughput_units[unit]


# Create or open the results file for writing
with open("results.txt", "w", encoding="utf-8"):
    # List files in the folder
    file_list = list(os.listdir(FOLDER_PATH))

    # Process each file
    for file_name in file_list:
        if file_name.startswith("."):
            continue
        file_path = os.path.join(FOLDER_PATH, file_name)

        # Read the data from the file
        with open(file_path, "r", encoding="utf-8") as current_file:
            lines = current_file.readlines()

        summary = lines[-1]

        data = summary.split()
        MIN_LATENCY = min(MIN_LATENCY, convert_to_ms(data[4]))
        SUM_AVG_LATENCY += convert_to_ms(data[5])
        MAX_LATENCY = max(MAX_LATENCY, convert_to_ms(data[6]))
        SUM_THROUGHPUT += convert_to_gib_per_second(data[7])
        SUM_ERRORS += int(data[8])
        COUNT += 1

# Calculate the average of average latencies
avg_of_avg_latency = SUM_AVG_LATENCY / COUNT

# Print the results
print(f"Minimum Latency (ms): {MIN_LATENCY:.3f}")
print(f"Average of Average Latencies (ms): {avg_of_avg_latency:.3f}")
print(f"Maximum Latency (ms): {MAX_LATENCY:.3f}")
print(f"Summation of all Throughputs (GiB/s): {SUM_THROUGHPUT:.2f}")
print(f"Summation of all Errors: {SUM_ERRORS}")
