#!/bin/bash

# Randomly delete 10% of the generated files
#
# See related:
# - ais/test/scripts/gen_nested_dirs.sh

if [ -z "$1" ]; then
    echo "Error: root directory is missing"
    exit 1
fi
root_dir="$1"
if [[ "$root_dir" != /tmp/* ]]; then
    echo "Error: expecting root directory to be under /tmp"
    exit 1
fi

all_files=( $(find "$root_dir" -type f) )
total_files=${#all_files[@]}
num_files_to_delete=$((total_files / 10))
deleted_files=()
for ((i=0; i<num_files_to_delete; i++)); do
    random_index=$((RANDOM % total_files))
    file_to_delete=${all_files[$random_index]}
    rm "$file_to_delete"
    deleted_files+=("$file_to_delete")
    # Remove the deleted file from array to avoid re-deletion
    all_files=("${all_files[@]:0:$random_index}" "${all_files[@]:$((random_index + 1))}")
    total_files=${#all_files[@]}
done

# Return the deleted pathnames
echo "Deleted files:"
for file in "${deleted_files[@]}"; do
    echo "$file"
done
