#!/bin/bash

if [ -z "$1" ]; then
  echo "Error: AIS endpoint URL not provided."
  echo "Usage: ./reset_buckets.sh <ais_endpoint_url> <bucket_names>"
  exit 1
fi

if [ -z "$2" ]; then
  echo "Error: List of bucket names not provided."
  echo "Usage: ./reset_buckets.sh <ais_endpoint_url> <bucket_names>"
  exit 1
fi

export AIS_ENDPOINT="$1"
BUCKET_NAMES=$(echo "$2" | tr ',' ' ')

for bucket in $BUCKET_NAMES; do
  ais bucket rm "ais://$bucket" -y
  ais bucket create "ais://$bucket"
done
