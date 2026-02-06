#!/bin/bash

#############################################
# Get AIStore endpoint for a running job
# Usage: ./get_endpoint.sh <job_id>
#############################################

JOB_ID=${1}
LUSTRE_BASE="/lustre/fsw/portfolios/av/users/abhgaikwad/aistore"

if [ -z "${JOB_ID}" ]; then
    echo "Usage: $0 <job_id>"
    echo ""
    echo "Your running jobs:"
    squeue -u $USER --format="%.10i %.20j %.8T %.10M %.6D %R"
    exit 1
fi

ENDPOINT_FILE="${LUSTRE_BASE}/jobs/${JOB_ID}/endpoint.txt"

if [ -f "${ENDPOINT_FILE}" ]; then
    ENDPOINT=$(cat ${ENDPOINT_FILE})
    echo "AIStore Endpoint: ${ENDPOINT}"
    echo ""
    echo "To use:"
    echo "  export AIS_ENDPOINT=${ENDPOINT}"
    echo "  ais show cluster"
else
    echo "Endpoint file not found for job ${JOB_ID}"
    echo "Job may still be starting up, or job ID is incorrect."
    exit 1
fi
