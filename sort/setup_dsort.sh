#!/bin/bash

DIR=../gen/data
EXT=".tar"
BUCKET="dsort-testing"

curl -i -X POST -H 'Content-Type: application/json' -d '{"action": "createlb"}' http://localhost:8080/v1/buckets/$BUCKET

i=0
for filename in $DIR/*$EXT; do
    base=`basename $filename`
    curl -L -X PUT http://localhost:8080/v1/objects/$BUCKET/$base -T $filename && echo "PUT ${base}"
    ((i++))
done
