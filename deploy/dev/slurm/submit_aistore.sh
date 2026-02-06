#!/bin/bash

#############################################
# AIStore Slurm Auto-Deploy Launcher
# Usage: ./submit_aistore.sh <num_nodes> [time] [partition] [account]
# Example: ./submit_aistore.sh 4 4:00:00 batch my_account
#############################################

# Default values - UPDATE THESE FOR YOUR ENVIRONMENT
DEFAULT_PARTITION="batch"           # Your Slurm partition name
DEFAULT_ACCOUNT="<YOUR_ACCOUNT>"    # Your Slurm account/project name

NUM_NODES=${1:-2}
TIME=${2:-"4:00:00"}
PARTITION=${3:-"$DEFAULT_PARTITION"}
ACCOUNT=${4:-"$DEFAULT_ACCOUNT"}

if [ -z "$1" ]; then
    echo "Usage: $0 <num_nodes> [time] [partition] [account]"
    echo "Example: $0 4 4:00:00 batch my_account"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "============================================"
echo "Submitting AIStore cluster job"
echo "  Nodes:     ${NUM_NODES}"
echo "  Time:      ${TIME}"
echo "  Partition: ${PARTITION}"
echo "  Account:   ${ACCOUNT}"
echo "============================================"

JOB_OUTPUT=$(sbatch \
    --job-name=aistore-cluster \
    --nodes=${NUM_NODES} \
    --ntasks-per-node=1 \
    --gpus-per-node=4 \
    --time=${TIME} \
    --partition=${PARTITION} \
    --account=${ACCOUNT} \
    --exclusive \
    --output=aistore_%j.out \
    --error=aistore_%j.err \
    --export=ALL \
    ${SCRIPT_DIR}/aistore_cluster.sh)

# Extract job ID from "Submitted batch job 12345"
JOB_ID=$(echo ${JOB_OUTPUT} | awk '{print $4}')

echo ""
echo "${JOB_OUTPUT}"
echo ""
echo "Job ID: ${JOB_ID}"
echo "Check status with: squeue -u $USER"
echo "Check output with: tail -f aistore_${JOB_ID}.out"
echo "Get endpoint with: ./get_endpoint.sh ${JOB_ID}"
