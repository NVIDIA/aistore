# AIStore MCP Server

[Model Context Protocol](https://modelcontextprotocol.io/) (MCP) server for AIStore, enabling AI agents (Claude, Cursor, etc.) to inspect and monitor AIStore clusters through a standardized interface.

## Quick Start

```bash
# Install dependencies
pip install aistore[mcp]

# Set your AIStore endpoint
export AIS_ENDPOINT="http://localhost:8080"

# Run the server
python -m aistore.mcp
```

## Available Tools

### Cluster Tools

| Tool | Description |
|------|-------------|
| `ais_cluster_health` | Check overall cluster health: readiness, node counts, primary proxy info |
| `ais_cluster_info` | Get detailed cluster map with all target and proxy nodes |
| `ais_cluster_performance` | Get current throughput, latency, and I/O counters from all targets |
| `ais_is_ready` | Quick check if the cluster is ready and reachable |

### Bucket Tools

| Tool | Description |
|------|-------------|
| `ais_list_buckets` | List all buckets in the cluster (filter by provider) |
| `ais_bucket_summary` | Get bucket summary: object count, total size, etc. |
| `ais_list_objects` | List objects in a bucket with optional prefix filtering |
| `ais_object_info` | Get metadata/properties for a specific object |

### Jobs & ETL Tools

| Tool | Description |
|------|-------------|
| `ais_list_jobs` | List all jobs in the cluster with their status |
| `ais_running_jobs` | List currently running jobs |
| `ais_job_status` | Get detailed status of a specific job |
| `ais_list_etls` | List all ETL transformers in the cluster |
| `ais_etl_details` | Get ETL details including object-level errors |
| `ais_etl_logs` | Get logs from ETL pods (base64-decoded) |

## Configuration

The server reads the AIStore endpoint from the `AIS_ENDPOINT` environment variable (default: `http://localhost:8080`).

### Claude Desktop

Add to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "aistore": {
      "command": "python",
      "args": ["-m", "aistore.mcp"],
      "env": {
        "AIS_ENDPOINT": "http://localhost:8080"
      }
    }
  }
}
```

### Claude Code

Add to your project's `.claude/settings.json` or use the `/mcp` command:

```json
{
  "mcpServers": {
    "aistore": {
      "command": "python",
      "args": ["-m", "aistore.mcp"],
      "env": {
        "AIS_ENDPOINT": "http://localhost:8080"
      }
    }
  }
}
```

## Safety and Scope

This MCP server is **strictly read-only**. It does not expose any tools that create, modify, or delete objects, buckets, or cluster configuration. All tools are observational and safe to use in automated agent workflows.
