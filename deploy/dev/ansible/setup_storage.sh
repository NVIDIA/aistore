#!/bin/bash

# Check if three arguments are provided
if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <string path of base dir> <comma-separated list of device names> <comma-separated list of mountpaths>"
  exit 1
fi

# Assign arguments to variables
base_dir=$1
devices=$2
mountpaths=$3

# Make directory base_dir
mkdir -p "$base_dir"
echo "Base directory created: $base_dir"

# Split the comma-separated lists into arrays
IFS=',' read -r -a devices <<< "$devices"
IFS=',' read -r -a mountpaths <<< "$mountpaths"

# Ensure devices and mountpaths lists are the same length
if [ ${#devices[@]} -ne ${#mountpaths[@]} ]; then
  echo "Error: The number of devices and mountpaths should be the same."
  exit 1
fi

for ((i = 0; i < ${#devices[@]}; i++)); do
  nested_dir="${base_dir}/${mountpaths[i]}"
  mkdir -p "$nested_dir"
  echo "Nested directory created: $nested_dir"

  # Mount the device to the corresponding mountpath
  sudo mount "${devices[i]}" "$nested_dir"
  echo "Device ${devices[i]} mounted to $nested_dir"
done
