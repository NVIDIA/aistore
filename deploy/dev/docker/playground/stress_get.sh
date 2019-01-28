#!/bin/bash

if ! [ -x "$(command -v jq)" ]; then
  echo "Error: jq is not installed." >&2
  exit 1
fi

OBJECTS=`curl -s -X POST http://172.50.0.2:8080/v1/buckets/$1 -H 'Content-Type: application/json' -d '{"action":"listobjects","value":{"props": "name"}}' | jq -r '.entries[].name'`

echo "Getting all objects from $1 bucket, each 100 times..."
for run in {1..30}; do
    echo "Batch $run..."
    for obj in $OBJECTS; do
        curl -s -L -X GET http://172.50.0.2:8080/v1/objects/$1/$obj > /dev/null &
    done

    for job in `jobs -p`; do
       wait $job || let "FAIL+=1"
    done
done