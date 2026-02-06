#!/bin/bash

#############################################
# AIStore Multi-Node Cluster Setup (Slurm)
# This script runs on ALL nodes via srun
#############################################

#SBATCH --job-name=aistore-cluster
#SBATCH --output=aistore_%j.out
#SBATCH --error=aistore_%j.err

set -e

#############################################
# CONFIGURATION - UPDATE THESE PATHS
#############################################
# LUSTRE_BASE: Shared filesystem path (accessible from all nodes) for storing configs, logs, and job metadata
# BIN_DIR:     Path to AIStore binaries (aisnode, ais) on shared filesystem
# DATA_PATH:   Node-local fast storage path for caching data (NVMe, SSD, RAID scratch)

LUSTRE_BASE="<SHARED_FS_PATH>/aistore"      # e.g., /lustre/users/$USER/aistore, /gpfs/home/$USER/aistore
BIN_DIR="<SHARED_FS_PATH>/aistore/bin"      # e.g., /lustre/users/$USER/aistore/bin
DATA_PATH="<LOCAL_SCRATCH_PATH>/aistore"    # e.g., /raid/scratch/aistore, /mnt/nvme0/aistore

# Binaries (absolute paths)
AISNODE="${BIN_DIR}/aisnode"
AIS_CLI="${BIN_DIR}/ais"

# AWS Backend - picks up from environment (passed via --export=ALL in sbatch)
# Or uses ~/.aws/credentials if available
# Set these before running submit_aistore.sh:
#   export AWS_ACCESS_KEY_ID="your-access-key"
#   export AWS_SECRET_ACCESS_KEY="your-secret-key"
#   export AWS_REGION="us-east-1"
if [ -n "${AWS_ACCESS_KEY_ID}" ]; then
    echo "AWS credentials detected from environment"
fi

# Check if aisnode exists
if [ ! -f "${AISNODE}" ]; then
    echo "ERROR: aisnode binary not found at ${AISNODE}"
    echo "Please build it first with: AIS_BACKEND_PROVIDERS=aws make node"
    echo "Then copy to: ${BIN_DIR}/"
    exit 1
fi

# Ports
PUBLIC_PORT=51080
CONTROL_PORT=52080
DATA_PORT=53080
TARGET_PUBLIC_PORT=51081
TARGET_CONTROL_PORT=52081
TARGET_DATA_PORT=53081

echo "============================================"
echo "AIStore Cluster Deployment"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Nodes:  ${SLURM_JOB_NUM_NODES}"
echo "============================================"

# Get list of all nodes in this job
NODELIST=$(scontrol show hostnames ${SLURM_JOB_NODELIST})
NODES_ARRAY=(${NODELIST})
NUM_NODES=${#NODES_ARRAY[@]}

echo "Nodes in this job:"
for i in "${!NODES_ARRAY[@]}"; do
    echo "  Node $i: ${NODES_ARRAY[$i]}"
done

# Create a shared config directory for this job
JOB_CONFIG_DIR="${LUSTRE_BASE}/jobs/${SLURM_JOB_ID}"
mkdir -p ${JOB_CONFIG_DIR}

# Get IP addresses for all nodes and save to shared file
echo "Discovering node IPs..."
srun --ntasks-per-node=1 bash -c '
    NODE_IP=$(hostname -I | awk "{print \$1}")
    HOSTNAME=$(hostname)
    echo "${HOSTNAME}:${NODE_IP}"
' | sort > ${JOB_CONFIG_DIR}/node_ips.txt

echo ""
echo "Node IP mapping:"
cat ${JOB_CONFIG_DIR}/node_ips.txt

# Determine primary node (first node)
PRIMARY_HOSTNAME=${NODES_ARRAY[0]}
PRIMARY_IP=$(grep "^${PRIMARY_HOSTNAME}:" ${JOB_CONFIG_DIR}/node_ips.txt | cut -d: -f2)

echo ""
echo "Primary node: ${PRIMARY_HOSTNAME} (${PRIMARY_IP})"
echo "Primary endpoint: http://${PRIMARY_IP}:${PUBLIC_PORT}"

# Save primary info
echo "${PRIMARY_IP}" > ${JOB_CONFIG_DIR}/primary_ip.txt
echo "http://${PRIMARY_IP}:${PUBLIC_PORT}" > ${JOB_CONFIG_DIR}/endpoint.txt

# Create a node startup script that will run on each node
NODE_SCRIPT="${JOB_CONFIG_DIR}/start_node.sh"
cat > ${NODE_SCRIPT} << 'NODESCRIPT'
#!/bin/bash
set -e

# AWS credentials are inherited from parent via --export=ALL

JOB_CONFIG_DIR="$1"
AISNODE="$2"
PUBLIC_PORT="$3"
CONTROL_PORT="$4"
DATA_PORT="$5"
TARGET_PUBLIC_PORT="$6"
TARGET_CONTROL_PORT="$7"
TARGET_DATA_PORT="$8"
LUSTRE_BASE="$9"
SLURM_JOB_ID="${10}"
DATA_PATH="${11}"

HOSTNAME=$(hostname)
NODE_IP=$(hostname -I | awk '{print $1}')
PRIMARY_IP=$(cat ${JOB_CONFIG_DIR}/primary_ip.txt)

# Determine node index
NODE_INDEX=$(grep -n "^${HOSTNAME}:" ${JOB_CONFIG_DIR}/node_ips.txt | cut -d: -f1)
NODE_INDEX=$((NODE_INDEX - 1))

echo "[${HOSTNAME}] Starting AIStore (Node ${NODE_INDEX}, IP: ${NODE_IP})"

# Create directories
NODE_CONFIG_DIR="${JOB_CONFIG_DIR}/node_${NODE_INDEX}"
NODE_LOG_DIR="${LUSTRE_BASE}/logs/job_${SLURM_JOB_ID}/node_${NODE_INDEX}"

mkdir -p ${NODE_CONFIG_DIR}/proxy
mkdir -p ${NODE_CONFIG_DIR}/target
mkdir -p ${NODE_LOG_DIR}/proxy
mkdir -p ${NODE_LOG_DIR}/target
mkdir -p ${DATA_PATH}

# Generate proxy global config
cat > ${NODE_CONFIG_DIR}/proxy/ais.json << PROXYEOF
{
    "backend": {"aws": {}},
    "mirror": {"copies": 2, "burst_buffer": 512, "enabled": false},
    "ec": {"objsize_limit": 262144, "compression": "never", "bundle_multiplier": 2, "data_slices": 1, "parity_slices": 1, "enabled": false, "disk_only": false},
    "log": {"level": "3", "max_size": "10mb", "max_total": "256mb", "flush_time": "60s", "stats_time": "60s"},
    "periodic": {"stats_time": "10s", "notif_time": "30s", "retry_sync_time": "2s"},
    "timeout": {"cplane_operation": "2s", "max_keepalive": "5s", "cold_get_conflict": "5s", "max_host_busy": "20s", "startup_time": "1m", "join_startup_time": "3m", "send_file_time": "5m", "ec_streams_time": "10m", "object_md": "2h"},
    "client": {"client_timeout": "10s", "client_long_timeout": "10m", "list_timeout": "1m"},
    "proxy": {"primary_url": "http://${PRIMARY_IP}:${PUBLIC_PORT}", "original_url": "http://${PRIMARY_IP}:${PUBLIC_PORT}", "discovery_url": "", "non_electable": false},
    "space": {"cleanupwm": 65, "lowwm": 75, "highwm": 90, "out_of_space": 95, "batch_size": 32768, "dont_cleanup_time": "120m"},
    "lru": {"dont_evict_time": "120m", "capacity_upd_time": "10m", "batch_size": 32768, "enabled": true},
    "disk": {"iostat_time_long": "2s", "iostat_time_short": "100ms", "iostat_time_smooth": "8s", "disk_util_low_wm": 20, "disk_util_high_wm": 80, "disk_util_max_wm": 95},
    "rebalance": {"dest_retry_time": "2m", "compression": "never", "bundle_multiplier": 2, "burst_buffer": 1024, "enabled": true},
    "resilver": {"enabled": true},
    "checksum": {"type": "xxhash2", "validate_cold_get": false, "validate_warm_get": false, "validate_obj_move": false, "enable_read_range": false},
    "transport": {"max_header": 4096, "burst_buffer": 512, "idle_teardown": "4s", "quiescent": "10s", "lz4_block": "256kb", "lz4_frame_checksum": false},
    "memsys": {"min_free": "2gb", "default_buf": "32kb", "to_gc": "4gb", "hk_time": "3m", "min_pct_total": 0, "min_pct_free": 0},
    "versioning": {"enabled": true, "validate_warm_get": false},
    "net": {"l4": {"proto": "tcp", "sndrcv_buf_size": 131072}, "http": {"use_https": false, "server_crt": "server.crt", "server_key": "server.key", "domain_tls": "", "client_ca_tls": "", "client_auth_tls": 0, "idle_conn_time": "6s", "idle_conns_per_host": 32, "idle_conns": 256, "write_buffer_size": 65536, "read_buffer_size": 65536, "chunked_transfer": true, "skip_verify": false}},
    "fshc": {"test_files": 4, "error_limit": 2, "io_err_limit": 10, "io_err_time": "10s", "enabled": true},
    "auth": {"signature": {"key": "", "method": "HMAC"}, "enabled": false},
    "keepalivetracker": {"proxy": {"interval": "10s", "name": "heartbeat", "factor": 3}, "target": {"interval": "10s", "name": "heartbeat", "factor": 3}, "num_retries": 3, "retry_factor": 4},
    "downloader": {"timeout": "1h"},
    "distributed_sort": {"duplicated_records": "ignore", "missing_shards": "ignore", "ekm_malformed_line": "abort", "ekm_missing_key": "abort", "default_max_mem_usage": "80%", "call_timeout": "10m", "dsorter_mem_threshold": "100GB", "compression": "never", "bundle_multiplier": 4},
    "write_policy": {"data": "", "md": ""},
    "features": "0"
}
PROXYEOF

# Copy for target (same global config)
cp ${NODE_CONFIG_DIR}/proxy/ais.json ${NODE_CONFIG_DIR}/target/ais.json

# Generate proxy local config
cat > ${NODE_CONFIG_DIR}/proxy/ais_local.json << PLOCALEOF
{
    "confdir": "${NODE_CONFIG_DIR}/proxy",
    "log_dir": "${NODE_LOG_DIR}/proxy",
    "host_net": {
        "hostname": "${NODE_IP}",
        "hostname_intra_control": "${NODE_IP}",
        "hostname_intra_data": "${NODE_IP}",
        "port": "${PUBLIC_PORT}",
        "port_intra_control": "${CONTROL_PORT}",
        "port_intra_data": "${DATA_PORT}"
    },
    "fspaths": {}
}
PLOCALEOF

# Generate target local config
cat > ${NODE_CONFIG_DIR}/target/ais_local.json << TLOCALEOF
{
    "confdir": "${NODE_CONFIG_DIR}/target",
    "log_dir": "${NODE_LOG_DIR}/target",
    "host_net": {
        "hostname": "${NODE_IP}",
        "hostname_intra_control": "${NODE_IP}",
        "hostname_intra_data": "${NODE_IP}",
        "port": "${TARGET_PUBLIC_PORT}",
        "port_intra_control": "${TARGET_CONTROL_PORT}",
        "port_intra_data": "${TARGET_DATA_PORT}"
    },
    "fspaths": {
        "${DATA_PATH}": ""
    }
}
TLOCALEOF

# Determine if this is the primary node
IS_PRIMARY=false
if [ "${NODE_IP}" == "${PRIMARY_IP}" ]; then
    IS_PRIMARY=true
fi

# Start proxy
if [ "${IS_PRIMARY}" == "true" ]; then
    echo "[${HOSTNAME}] Starting PRIMARY proxy..."
    ${AISNODE} \
        -config=${NODE_CONFIG_DIR}/proxy/ais.json \
        -local_config=${NODE_CONFIG_DIR}/proxy/ais_local.json \
        -role=proxy \
        -ntargets=${NUM_NODES:-4} \
        &
    PROXY_PID=$!
else
    # Wait for primary to be ready
    echo "[${HOSTNAME}] Waiting for primary proxy..."
    sleep 15
    echo "[${HOSTNAME}] Starting proxy (joining cluster)..."
    ${AISNODE} \
        -config=${NODE_CONFIG_DIR}/proxy/ais.json \
        -local_config=${NODE_CONFIG_DIR}/proxy/ais_local.json \
        -role=proxy \
        &
    PROXY_PID=$!
fi

echo "[${HOSTNAME}] Proxy PID: ${PROXY_PID}"

# Wait a bit before starting target
sleep 10

# Start target
echo "[${HOSTNAME}] Starting target..."
${AISNODE} \
    -config=${NODE_CONFIG_DIR}/target/ais.json \
    -local_config=${NODE_CONFIG_DIR}/target/ais_local.json \
    -role=target \
    &
TARGET_PID=$!

echo "[${HOSTNAME}] Target PID: ${TARGET_PID}"

# Save PIDs
echo "${PROXY_PID}" > ${NODE_CONFIG_DIR}/proxy.pid
echo "${TARGET_PID}" > ${NODE_CONFIG_DIR}/target.pid

echo "[${HOSTNAME}] AIStore started, waiting for processes..."

# Wait for both processes - this keeps the script running
wait ${PROXY_PID} ${TARGET_PID}
NODESCRIPT

chmod +x ${NODE_SCRIPT}

# Start AIStore on all nodes using srun
# Each node will run the script and wait for the processes
echo ""
echo "Starting AIStore on all nodes..."

srun --ntasks-per-node=1 --unbuffered --export=ALL bash ${NODE_SCRIPT} \
    "${JOB_CONFIG_DIR}" \
    "${AISNODE}" \
    "${PUBLIC_PORT}" \
    "${CONTROL_PORT}" \
    "${DATA_PORT}" \
    "${TARGET_PUBLIC_PORT}" \
    "${TARGET_CONTROL_PORT}" \
    "${TARGET_DATA_PORT}" \
    "${LUSTRE_BASE}" \
    "${SLURM_JOB_ID}" \
    "${DATA_PATH}" &

SRUN_PID=$!

# Wait for cluster to stabilize
echo ""
echo "Waiting for cluster to stabilize..."
sleep 30

# Print cluster info
echo ""
echo "============================================"
echo "AIStore Cluster Ready!"
echo "============================================"
echo ""
echo "Job ID:       ${SLURM_JOB_ID}"
echo "Nodes:        ${NUM_NODES}"
echo "Primary:      http://${PRIMARY_IP}:${PUBLIC_PORT}"
echo ""
echo "Config dir:   ${JOB_CONFIG_DIR}"
echo "Logs dir:     ${LUSTRE_BASE}/logs/job_${SLURM_JOB_ID}"
echo ""
echo "To use the cluster:"
echo "  export AIS_ENDPOINT=http://${PRIMARY_IP}:${PUBLIC_PORT}"
echo "  ais show cluster"
echo ""
echo "Endpoint file: ${JOB_CONFIG_DIR}/endpoint.txt"
echo ""
echo "Cluster is running. Cancel the job to stop."
echo "============================================"

export AIS_ENDPOINT=http://${PRIMARY_IP}:${PUBLIC_PORT}
${AIS_CLI} show cluster
# Wait for srun to finish (keeps job alive)
wait ${SRUN_PID}
