#!/bin/bash

export PATH=$PATH:/usr/local/go/bin

if [ "$#" -ne 6 ]; then
  echo "Usage: $0 <source_root> <directory> <fs_paths> <primary_host> <proxy_cnt> <port>"
  exit 1
fi

source_root="$1"
directory="$2"
fs_paths="$3"
primary_host="$4"
proxy_cnt="$5"
port="$6"

# Convert the comma-separated paths to an array
IFS=',' read -r -a fs_paths_array <<< "$fs_paths"

# Create the AIS_FS_PATHS config entry
FSP=()
for disk in "${fs_paths_array[@]}"; do
  FSP+=("\"$directory/$disk\": {}")
done

FSP=$(printf ", %s" "${FSP[@]}")
# Remove the leading comma and space
FSP=${FSP:2}

# Set the env variables
export AIS_PRIMARY_HOST="$primary_host"
export AIS_FS_PATHS="$FSP"
export PORT="$port"
export GOPATH=~/go

echo "Run config:"
echo "FS paths: $AIS_FS_PATHS"
echo "Port: $PORT"
echo "Primary host: $AIS_PRIMARY_HOST"

cd "$source_root"

# Kill old and deploy new
make cli && make kill
echo -e "1\n$proxy_cnt\n0\nn\nn\nn\nn\n" | make deploy