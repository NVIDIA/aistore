#!/bin/bash

LOG_ROOT="/tmp/ais"
TMP_FILE="$LOG_ROOT/ais_cov"

echo 'mode: count' > ${TMP_FILE}
tail -q -n +2 "${LOG_ROOT}/*.cov" >> ${TMP_FILE}
go tool cover -html=${TMP_FILE} -o "${LOG_ROOT}/ais_cov.html"
rm ${TMP_FILE}
