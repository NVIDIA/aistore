#!/bin/bash

# =============================================================================
# Script: gen-nested-dirs.sh
# Description: Generates a nested directory structure with randomly named zero-size files.
# Usage: ./gen-nested-dirs.sh [OPTIONS]
#
# Options:
#   --root_dir  DIR    Root directory for test environment (default: /tmp/ais-<PID>-test)
#   --num_dirs  N      Number of nested directories to create (default: 13)
#   --max_depth N      Maximum depth of nesting (default: 2)
#   --num_files N      Number of files to create in the root and each nested directory (default: 100)
#   --len_fname N      Length of randomly generated filenames (default: 5)
#   --help, -h         Display this help message
#
# See related:
#   - ais/test/scripts/remais-del-list-deleted.sh
# =============================================================================

# Defaults
root_dir="/tmp/ais-$$-test"
num_dirs=13
max_depth=2
num_files=100
len_fname=5

# usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --root_dir DIR   Root directory for test environment (default: /tmp/ais-<PID>-test)"
    echo "  --num_dirs N     Number of nested directories to create (default: 13)"
    echo "  --max_depth N    Maximum depth of nesting (default: 2)"
    echo "  --num_files N    Number of files to create in the root and each nested directory (default: 100)"
    echo "  --len_fname N    Length of randomly generated filenames (default: 5)"
    echo "  -h, --help       Display this help message"
    exit 1
}

# random name
generate_random_name() {
    local length=$len_fname
    local chars=({a..z})
    local name=""
    local i
    for ((i=0; i<length; i++)); do
        name+=${chars[RANDOM % ${#chars[@]}]}
    done
    echo "$name"
}

# parse command line
while (( "$#" )); do
  case "${1}" in
    --root_dir) root_dir=$2; shift; shift;;
    --num_dirs) num_dirs=$2; shift; shift;;
    --max_depth) max_depth=$2; shift; shift;;
    --num_files) num_files=$2; shift; shift;;
    --len_fname) len_fname=$2; shift; shift;;
    -h|--help) usage; exit 0;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

# create/confirm root dir
create_dir=true
if [ -d "$root_dir" ]; then
    create_dir=false
    if [ ! -z "$(ls -A "$root_dir")" ]; then
        read -p "Directory $root_dir already exists. Remove it? (y/n): " confirm
        if [[ "$confirm" =~ ^[Yy]$ ]]; then
            echo "Removing existing directory: $root_dir"
            rm -rf "$root_dir"
            create_dir=true
        else
            echo "Exiting without making changes."
            exit 1
        fi
    fi
fi
if [[ "$create_dir" == "true" ]]; then
    echo "Creating root directory: $root_dir"
    mkdir -p "$root_dir" || { echo "Failed to create root directory."; exit 1; }
fi

dir_count=0

# main recursion
create_subtree() {
    local current_dir="$1"
    local current_depth="$2"

    if [ $current_depth -gt $max_depth ]; then
        return
    fi

    local remaining_dirs=$((num_dirs - dir_count))
    local dirs_to_create=0
    local max_possible_dirs=$(min "$remaining_dirs" 5)
    if [ "$max_possible_dirs" -le 1 ]; then
        return
    fi

    dirs_to_create=$(( RANDOM % max_possible_dirs + 1 ))

    # create subdirectories
    local i
    for ((i=0; i<dirs_to_create; i++)); do
        local dir_name
        dir_name=$(generate_random_name)
        local new_dir="$current_dir/$dir_name"

        mkdir -p "$new_dir" || { echo "Failed to create directory: $new_dir"; exit 1; }
        dir_count=$((dir_count + 1))
        echo "$new_dir ..."

	# recurs
        create_subtree "$new_dir" $((current_depth + 1))
    done
}

# create files in a given directory
create_files_in_dir() {
    local target_dir="$1"
    local files_to_create="$2"

    local i
    for ((i=0; i<files_to_create; i++)); do
        local file_name
        file_name=$(generate_random_name).txt
        local new_file="$target_dir/$file_name"

        > "$new_file" || { echo "Failed to create file: $new_file"; exit 1; }
    done
}

# min
min() {
    if [ "$1" -lt "$2" ]; then
        echo "$1"
    else
        echo "$2"
    fi
}

# main
echo "Generating directories..."
create_subtree "$root_dir" 0

echo "Creating files in each directory..."
# Directly iterate over the directories found by find
find "$root_dir" -mindepth 0 -type d | while read -r dir; do
    create_files_in_dir "$dir" "$num_files"
done

echo "Done."
dir_count=$((dir_count + 1)) ## including root
echo "Total directories created: $dir_count"
file_count=$(find "$root_dir" -type f | wc -l)
echo "Total files created: $file_count"
