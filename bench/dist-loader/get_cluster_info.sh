#!/bin/bash

if [ -z "$1" ]; then
  echo "Error: AIS endpoint URL not provided."
  echo "Usage: ./get_cluster_info.sh <ais_endpoint_url>"
  exit 1
fi

export AIS_ENDPOINT="$1"
ais show cluster > output/cluster_details.txt
echo -e "\nExporting cluster details to output/cluster_details.txt\n"