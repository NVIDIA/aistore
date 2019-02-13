#!/bin/bash

if ! [ -x "$(command -v jq)" ]; then
  echo "Error: jq is not installed." >&2
  exit 1
fi

OBJECTS=`curl -s -X POST http://172.50.0.2:8080/v1/buckets/$1 -H 'Content-Type: application/json' -d '{"action":"listobjects","value":{"props": "name"}}' | jq -r '.entries[].name'`

echo "Getting all objects from $1 bucket, each 40 times..."
for run in {1..40}; do
    echo "Batch $run..."
    for obj in $OBJECTS; do
        wget -O/dev/null -q http://172.50.0.2:8080/v1/objects/$1/$obj &
    done

    while test $(jobs -p|wc -w) -ge 500; do sleep 0.01; done
done