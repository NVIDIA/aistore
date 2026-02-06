#!/bin/bash

#############################################
# Stop AIStore on all nodes in a Slurm job
# Usage: ./stop_aistore.sh [job_id]
#############################################

JOB_ID=${1:-${SLURM_JOB_ID}}

if [ -z "${JOB_ID}" ]; then
    echo "Usage: $0 <job_id>"
    echo "Or run from within a Slurm job"
    exit 1
fi

echo "Stopping AIStore cluster for job ${JOB_ID}..."

# Cancel the Slurm job (this will kill all processes)
scancel ${JOB_ID}

echo "Job ${JOB_ID} canceled."
