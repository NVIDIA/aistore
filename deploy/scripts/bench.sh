#!/bin/bash
pushd ${AISTORE_DIR}/bench
BUCKET=${BUCKET} AIS_ENDPOINT=${AIS_ENDPOINT} go test -v -p 1 -parallel 4 -count 1 -timeout 2h  -bench=. ./... -benchtime=1x 2>&1 | tee -a /dev/stderr | grep -ae "^FAIL\|^--- FAIL"
popd
