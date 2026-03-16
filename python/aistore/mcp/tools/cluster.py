#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""Cluster health, info, and performance tools."""

import json
import os
from typing import Callable

from mcp.server.fastmcp import FastMCP


def register_cluster_tools(mcp: FastMCP, get_client: Callable):
    """Register all cluster-related MCP tools."""

    @mcp.tool()
    def ais_cluster_health() -> str:
        """Check overall AIStore cluster health.

        Returns cluster readiness, node counts, primary proxy info,
        and cluster UUID/version.
        """
        client = get_client()
        cluster = client.cluster()

        ready = cluster.is_ready()
        info = cluster.get_info()

        targets = info.tmap or {}
        proxies = info.pmap or {}

        result = {
            "status": "healthy" if ready else "unhealthy",
            "ready": ready,
            "uuid": info.uuid,
            "version": info.version,
            "targets": {
                "count": len(targets),
                "nodes": [
                    {
                        "id": node.daemon_id,
                        "url": node.public_net.direct_url if node.public_net else "",
                    }
                    for node in targets.values()
                ],
            },
            "proxies": {
                "count": len(proxies),
                "nodes": [
                    {
                        "id": node.daemon_id,
                        "url": node.public_net.direct_url if node.public_net else "",
                    }
                    for node in proxies.values()
                ],
            },
            "primary_proxy": {
                "id": info.proxy_si.daemon_id if info.proxy_si else "",
                "url": (
                    info.proxy_si.public_net.direct_url
                    if info.proxy_si and info.proxy_si.public_net
                    else ""
                ),
            },
        }
        return json.dumps(result, indent=2)

    @mcp.tool()
    def ais_cluster_info() -> str:
        """Get detailed cluster map with all target and proxy nodes.

        Returns full Smap including node IDs, network addresses, flags,
        and cluster metadata.
        """
        client = get_client()
        info = client.cluster().get_info()
        try:
            return json.dumps(info.model_dump(), indent=2, default=str)
        except AttributeError:
            return json.dumps({"raw": str(info)}, indent=2)

    @mcp.tool()
    def ais_cluster_performance() -> str:
        """Get current cluster performance metrics.

        Returns throughput, latency, and I/O counters from all target nodes.
        """
        client = get_client()
        perf = client.cluster().get_performance()
        return json.dumps(perf, indent=2, default=str)

    @mcp.tool()
    def ais_is_ready() -> str:
        """Quick check if the AIStore cluster is ready and reachable."""
        client = get_client()
        endpoint = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
        try:
            ready = client.cluster().is_ready()
            return json.dumps({"ready": ready, "endpoint": endpoint})
        except Exception as e:  # pylint: disable=broad-except
            return json.dumps({"ready": False, "endpoint": endpoint, "error": str(e)})
