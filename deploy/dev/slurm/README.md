# AIStore Automated Slurm Deployment

> **⚠️ EXPERIMENTAL**: This deployment method is experimental and intended for testing and development purposes. Use at your own risk in production environments. The scripts and configurations may change without notice.

Automatically deploy AIStore clusters on Slurm with any number of nodes.

---

## Table of Contents

- [Why Run AIStore on Slurm?](#why-run-aistore-on-slurm)
- [Prerequisites](#prerequisites)
- [Configuration](#configuration)
- [Quick Start](#quick-start)
- [How It Works](#how-it-works)
- [Reference](#reference)
- [Troubleshooting](#troubleshooting)

---

## Why Run AIStore on Slurm?

Slurm is typically used to submit training jobs on GPU clusters. Modern GPU nodes are increasingly equipped with large amounts of local storage—NVMe drives, scratch disks, or RAID arrays—that often go underutilized during training workloads.

**The Problem**: Training jobs frequently fetch data from remote object stores (S3, GCS, etc.) or shared filesystems (Lustre, GPFS), which can become bottlenecks due to network latency and bandwidth limitations.

**The Solution**: Deploy AIStore directly on the Slurm nodes to create a high-performance, fast-tier local storage layer:

1. **Allocate resources** — Request nodes with local storage via Slurm
2. **Deploy AIStore** — Run this deployment to spin up AIStore across all allocated nodes
3. **Get the endpoint** — Retrieve the AIStore endpoint URL once the cluster is ready
4. **Submit training jobs** — Point your training workloads to the AIStore endpoint

### Benefits

| Benefit | Description |
|---------|-------------|
| **Fast Tier Storage** | Data is stored on local NVMe/SSD drives, providing significantly faster read speeds compared to remote storage |
| **Reduced Network Load** | Frequently accessed data is served locally, reducing pressure on shared filesystems and network |
| **Seamless Integration** | AIStore is a fully compliant S3-compatible storage solution—your training code works with standard S3 APIs |
| **Scalable** | Add more nodes to increase both storage capacity and aggregate bandwidth |

### Architecture

AIStore runs on the **same GPU nodes** where your training job will execute. This co-location enables the training process to read data from local storage with minimal latency.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Slurm GPU Cluster                              │
│                                                                             │
│  ┌────────────────────┐  ┌────────────────────┐  ┌────────────────────┐     │
│  │   GPU Node 1       │  │   GPU Node 2       │  │   GPU Node 3       │     │
│  │  ┌──────────────┐  │  │  ┌──────────────┐  │  │  ┌──────────────┐  │     │
│  │  │   Training   │  │  │  │   Training   │  │  │  │   Training   │  │     │
│  │  │   Process    │  │  │  │   Process    │  │  │  │   Process    │  │     │
│  │  │  (uses GPU)  │  │  │  │  (uses GPU)  │  │  │  │  (uses GPU)  │  │     │
│  │  └──────┬───────┘  │  │  └──────┬───────┘  │  │  └──────┬───────┘  │     │
│  │         │ read     │  │         │ read     │  │         │ read     │     │
│  │  ┌──────▼───────┐  │  │  ┌──────▼───────┐  │  │  ┌──────▼───────┐  │     │
│  │  │   AIStore    │  │  │  │   AIStore    │  │  │  │   AIStore    │  │     │
│  │  │ proxy+target │◄─┼──┼──► proxy+target │◄─┼──┼──► proxy+target │  │     │
│  │  └──────┬───────┘  │  │  └──────┬───────┘  │  │  └──────┬───────┘  │     │
│  │         │          │  │         │          │  │         │          │     │
│  │  ┌──────▼───────┐  │  │  ┌──────▼───────┐  │  │  ┌──────▼───────┐  │     │
│  │  │  Local NVMe  │  │  │  │  Local NVMe  │  │  │  │  Local NVMe  │  │     │
│  │  │   /scratch   │  │  │  │   /scratch   │  │  │  │   /scratch   │  │     │
│  │  │  (fast tier) │  │  │  │  (fast tier) │  │  │  │  (fast tier) │  │     │
│  │  └──────────────┘  │  │  └──────────────┘  │  │  └──────────────┘  │     │
│  └────────────────────┘  └────────────────────┘  └────────────────────┘     │
│              │                     │                     │                  │
│              └─────────────────────┼─────────────────────┘                  │
│                                    │ If not found locally                   │
│                                    ▼                                        │
└────────────────────────────────────┼────────────────────────────────────────┘
                                     │
                              ┌──────▼──────┐
                              │   Remote    │
                              │   Storage   │
                              │     (S3)    │
                              └─────────────┘
```

**Data Flow**: Training jobs read from AIStore → AIStore fetches from remote storage if the object is not found locally → subsequent reads are served from fast local disks.

---

## Prerequisites

### 1. Build AIStore Binaries

Build the binaries with the backend providers you need (e.g., AWS, GCP, OCI, etc.):

```bash
cd $AISTORE_REPO
AIS_BACKEND_PROVIDERS=aws make node cli
```

> Replace `$AISTORE_REPO` with the path to your cloned [AIStore repository](https://github.com/NVIDIA/aistore).

### 2. Copy Binaries to Shared Filesystem

The binaries must be accessible from all Slurm nodes. Copy them to a shared filesystem (e.g., Lustre, GPFS, NFS):

```bash
mkdir -p $SHARED_FS/aistore/bin
cp bin/aisnode $SHARED_FS/aistore/bin/
cp bin/ais $SHARED_FS/aistore/bin/
```

> Replace `$SHARED_FS` with your shared filesystem path (e.g., `/lustre/users/$USER`, `/gpfs/home/$USER`, etc.)

### 3. (Optional) AWS Credentials

If using S3 as a backend, set these environment variables before submitting the job:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
export AWS_REGION="us-east-1"
```

---

## Configuration

> **⚠️ IMPORTANT**: You **must** edit `aistore_cluster.sh` and configure the following variables before running the deployment.

### Required Variables

Edit the top of `aistore_cluster.sh` to set these paths for your environment:

| Variable | Description | Example |
|----------|-------------|---------|
| `SHARED_FS_BASE` | Shared filesystem path for configs and logs | `$SHARED_FS/aistore` |
| `BIN_DIR` | Path where AIStore binaries are stored | `$SHARED_FS/aistore/bin` |
| `DATA_PATH` | Local disk path for storing data | `$LOCAL_SCRATCH/aistore/data` |

```bash
# Configuration paths (edit these in aistore_cluster.sh)
SHARED_FS_BASE="$SHARED_FS/aistore"           # e.g., /lustre/users/$USER/aistore
BIN_DIR="$SHARED_FS/aistore/bin"           # e.g., /lustre/users/$USER/aistore/bin
DATA_PATH="$LOCAL_SCRATCH/aistore/data"    # e.g., /raid/scratch/aistore/data
```

Where:
- `$SHARED_FS` = Your shared filesystem path accessible from all nodes (Lustre, GPFS, NFS, etc.)
- `$LOCAL_SCRATCH` = Node-local fast storage (NVMe, SSD, RAID scratch)

### Multiple Disks Configuration

If your nodes have **multiple independent disks** (e.g., multiple NVMe drives), you need to modify the `fspaths` configuration in `aistore_cluster.sh` to take advantage of all available storage.

The default configuration uses a single data path:

```json
"fspaths": {
    "${DATA_PATH}": ""
}
```

For nodes with multiple disks, update the target's local config section to include all disk paths:

```json
"fspaths": {
    "$LOCAL_DISK_1/aistore": "",
    "$LOCAL_DISK_2/aistore": "",
    "$LOCAL_DISK_3/aistore": "",
    "$LOCAL_DISK_4/aistore": ""
}
```

Example with actual paths:
```json
"fspaths": {
    "/mnt/nvme0/aistore": "",
    "/mnt/nvme1/aistore": "",
    "/mnt/nvme2/aistore": "",
    "/mnt/nvme3/aistore": ""
}
```

> **Note**: Each `fspath` should point to a separate physical disk or RAID volume. AIStore will stripe data across all configured paths for optimal performance.

### AIStore Configuration (ais.json)

The `aistore_cluster.sh` script generates `ais.json` configuration files with embedded settings for timeouts, logging, checksums, rebalancing, and other AIStore parameters.

> **⚠️ WARNING**: These embedded configurations are **not frequently updated** and may become outdated as AIStore evolves. If you are using a newer version of AIStore that introduces new configuration options or changes existing ones, you may need to manually update the `ais.json` template sections within `aistore_cluster.sh`.

To update the configuration:
1. Refer to the [AIStore configuration documentation](https://github.com/NVIDIA/aistore/blob/main/docs/configuration.md)
2. Locate the `cat > ${NODE_CONFIG_DIR}/proxy/ais.json` section in `aistore_cluster.sh`
3. Update the JSON configuration to match the expected format for your AIStore version

### Slurm Job Configuration

Edit the top of `submit_aistore.sh` to set default Slurm parameters for your environment:

```bash
# Default values - UPDATE THESE FOR YOUR ENVIRONMENT
DEFAULT_PARTITION="batch"           # Your Slurm partition name
DEFAULT_ACCOUNT="<YOUR_ACCOUNT>"    # Your Slurm account/project name
```

These defaults are used when you don't specify them on the command line:
```bash
./submit_aistore.sh 4                           # Uses DEFAULT_PARTITION and DEFAULT_ACCOUNT
./submit_aistore.sh 4 4:00:00 gpu my_account    # Overrides with custom values
```

### Port Configuration

The default ports can be changed if they conflict with other services:

| Service | Variable | Default Port |
|---------|----------|--------------|
| Proxy Public | `PUBLIC_PORT` | 51080 |
| Proxy Control | `CONTROL_PORT` | 52080 |
| Proxy Data | `DATA_PORT` | 53080 |
| Target Public | `TARGET_PUBLIC_PORT` | 51081 |
| Target Control | `TARGET_CONTROL_PORT` | 52081 |
| Target Data | `TARGET_DATA_PORT` | 53081 |

---

## Quick Start

### 1. Submit a Cluster Job

```bash

# Deploy 4-node cluster (default: 2 hours, batch partition)
./submit_aistore.sh 4

# Or with custom parameters: nodes, time, partition, account
./submit_aistore.sh 4 4:00:00 batch my_account
```

> Replace `$AIS_INFRA_REPO` with the path to your cloned ais-infra repository.

### 2. Check Job Status

```bash
squeue -u $USER
```

### 3. Get the Endpoint

Once the job is running, retrieve the AIStore endpoint:

```bash
./get_endpoint.sh <job_id>
```

### 4. Use the Cluster

```bash
# Set the endpoint
export AIS_ENDPOINT=http://<primary_ip>:51080

# Verify the cluster is healthy
ais show cluster

# Create a bucket and start using it
ais bucket create ais://mybucket
ais put myfile.txt ais://mybucket
```

### 5. Submit Your Training Job

Now submit your training job, pointing it to the AIStore endpoint:

```bash
# In your training script or job submission
export AIS_ENDPOINT=http://<primary_ip>:51080

# Your training code can now use AIStore as a fast-tier local storage
```

### 6. Stop the Cluster

```bash
./stop_aistore.sh <job_id>
# Or simply:
scancel <job_id>
```

---

## How It Works

1. **`submit_aistore.sh`** submits a Slurm job requesting N nodes
2. **`aistore_cluster.sh`** runs on all allocated nodes and:
   - Discovers IP addresses of all nodes via `srun`
   - Designates the first node as the primary proxy
   - Generates configuration files dynamically for each node
   - Starts a proxy and target daemon on each node
   - All nodes automatically join the cluster

### File Descriptions

| File | Description |
|------|-------------|
| `submit_aistore.sh` | Submits a new AIStore cluster job to Slurm |
| `aistore_cluster.sh` | Main deployment script (runs on each node) |
| `get_endpoint.sh` | Retrieves the endpoint URL for a running cluster |
| `stop_aistore.sh` | Gracefully stops a running cluster |

---

## Reference

### Directory Structure

After deployment, the following structure is created on the shared filesystem:

```
${SHARED_FS_BASE}/
├── bin/
│   ├── aisnode              # AIStore daemon binary
│   └── ais                  # AIStore CLI binary
├── jobs/
│   └── <job_id>/
│       ├── endpoint.txt     # Cluster endpoint URL
│       ├── primary_ip.txt   # Primary node IP address
│       ├── node_ips.txt     # All node IP addresses
│       ├── start_node.sh    # Generated node startup script
│       └── node_<N>/        # Per-node configuration
│           ├── proxy/
│           │   ├── ais.json
│           │   └── ais_local.json
│           └── target/
│               ├── ais.json
│               └── ais_local.json
└── logs/
    └── job_<job_id>/
        └── node_<N>/
            ├── proxy/
            │   └── aisnode.log
            └── target/
                └── aisnode.log
```

### Local Data Storage

Each node stores object data locally at the configured `DATA_PATH`:

```
${DATA_PATH}/   # e.g., /raid/scratch/aistore/data/ or your custom fspaths
```

---

## Troubleshooting

### Check Slurm Job Output

```bash
tail -f aistore_<job_id>.out
tail -f aistore_<job_id>.err
```

### Check AIStore Logs

```bash
# Proxy logs
tail -f ${SHARED_FS_BASE}/logs/job_<job_id>/node_0/proxy/aisnode.log

# Target logs
tail -f ${SHARED_FS_BASE}/logs/job_<job_id>/node_0/target/aisnode.log
```

### Verify Cluster Health

```bash
export AIS_ENDPOINT=$(cat ${SHARED_FS_BASE}/jobs/<job_id>/endpoint.txt)
ais show cluster
ais show cluster stats
```

### Common Issues

| Issue | Solution |
|-------|----------|
| `aisnode binary not found` | Ensure binaries are copied to `BIN_DIR` and the path is correct |
| `Connection refused` | Wait for the cluster to fully start (~30-60 seconds) |
| `Nodes not joining cluster` | Check network connectivity and firewall rules between nodes |
| `Disk full errors` | Ensure `DATA_PATH` has sufficient free space |
| `Port already in use` | Change the port configuration or ensure no other AIStore instances are running |

---

## Additional Resources

- [AIStore Documentation](https://github.com/NVIDIA/aistore)
- [AIStore CLI Reference](https://github.com/NVIDIA/aistore/blob/main/docs/cli.md)
- [Slurm Documentation](https://slurm.schedmd.com/documentation.html)
