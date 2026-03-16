#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""
AIStore MCP Server — lets AI agents interact with AIStore clusters.

Usage:
    # Start with default endpoint from AIS_ENDPOINT env var
    python -m aistore.mcp.server

    # Or specify endpoint directly
    AIS_ENDPOINT=http://localhost:8080 python -m aistore.mcp.server
"""

import os
from typing import Optional

from mcp.server.fastmcp import FastMCP

from aistore.sdk.client import Client
from aistore.mcp.tools.cluster import register_cluster_tools
from aistore.mcp.tools.buckets import register_bucket_tools

# Initialize MCP server
mcp = FastMCP(
    name="aistore",
    instructions=(
        "AIStore MCP Server for cluster management and debugging. "
        "Use these tools to check cluster health and list buckets. "
        "All tools are read-only."
    ),
)

_AIS_CLIENT: Optional[Client] = None


def _get_client() -> Client:
    """Get or create the AIS client for the current endpoint."""
    global _AIS_CLIENT  # pylint: disable=global-statement
    if _AIS_CLIENT is None:
        endpoint = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
        _AIS_CLIENT = Client(endpoint)
    return _AIS_CLIENT


# Register all tools
register_cluster_tools(mcp, _get_client)
register_bucket_tools(mcp, _get_client)


def main():
    """Entry point for the MCP server."""
    mcp.run()


if __name__ == "__main__":
    main()
