#!/bin/bash

###################################
#
# fspaths config is used if and only if test_fspath_cnt == 0
# existence of each fspath is checked at runtime
#
###################################

# Function to check if a value is a number
is_number() {
    if ! [[ $1 =~ ^[0-9]+$ ]]; then
        echo "Error: Not a valid number." >&2
        exit 1
    fi
}

echo "Select AIStore deployment type"
echo "  1: Development and test (emulated disks and mountpaths)"
echo "  2: Production (one or more formatted data drives, one drive per mountpath)"
echo "Enter your choice (1 or 2):"
read -r cache_source

if [[ $cache_source -eq 1 ]]; then
    echo "Enter the number of emulated disks/mountpaths:"
    read -r test_fspath_cnt
    is_number "$test_fspath_cnt"
elif [[ $cache_source -eq 2 ]]; then
    echo "Enter filesystem info in comma-separated format (e.g., /tmp/ais1,/tmp/ais):"
    read -r fsinfo
    fspath=""
    IFS=',' read -r -a array <<< "$fsinfo"
    for element in "${array[@]}"; do
        fspath="$fspath,\"$element\" : {} "
    done
    fspath=${fspath#","}
else
    echo "Invalid choice. Please select 1 or 2." >&2
    exit 1
fi

# Export variables
export AIS_FS_PATHS=${fspath}
export TEST_FSPATH_COUNT=${test_fspath_cnt}