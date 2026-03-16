#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""Bucket and object listing tools."""

import json
from typing import Callable

from mcp.server.fastmcp import FastMCP


def register_bucket_tools(mcp: FastMCP, get_client: Callable):
    """Register all bucket-related MCP tools."""

    @mcp.tool()
    def ais_list_buckets(provider: str = "ais") -> str:
        """List all buckets in the cluster.

        Args:
            provider: Bucket provider — "ais" (default), "aws", "gcp", "azure", "oci".
        """
        client = get_client()
        buckets = client.cluster().list_buckets(provider=provider)
        result = [{"name": b.name, "provider": b.provider} for b in buckets]
        return json.dumps(
            {"provider": provider, "count": len(result), "buckets": result},
            indent=2,
        )

    @mcp.tool()
    def ais_bucket_summary(bucket_name: str, provider: str = "ais") -> str:
        """Get summary of a specific bucket (object count, total size, etc.).

        Args:
            bucket_name: Name of the bucket.
            provider: Bucket provider — "ais" (default), "aws", "gcp", etc.
        """
        client = get_client()
        bck = client.bucket(bucket_name, provider=provider)
        _, summary = bck.info()
        return json.dumps(
            {"bucket": bucket_name, "provider": provider, "summary": summary},
            indent=2,
            default=str,
        )

    @mcp.tool()
    def ais_list_objects(
        bucket_name: str,
        provider: str = "ais",
        prefix: str = "",
        limit: int = 100,
    ) -> str:
        """List objects in a bucket.

        Args:
            bucket_name: Name of the bucket.
            provider: Bucket provider — "ais" (default), "aws", "gcp", etc.
            prefix: Only list objects with this prefix.
            limit: Maximum number of objects to return (default 100).
        """
        client = get_client()
        bck = client.bucket(bucket_name, provider=provider)
        entries = bck.list_objects(prefix=prefix, page_size=limit)

        objects = [
            {"name": entry.name, "size": entry.size}
            for entry in entries.entries[:limit]
        ]

        return json.dumps(
            {
                "bucket": bucket_name,
                "prefix": prefix,
                "count": len(objects),
                "objects": objects,
            },
            indent=2,
        )

    @mcp.tool()
    def ais_object_info(
        bucket_name: str, object_name: str, provider: str = "ais"
    ) -> str:
        """Get metadata for a specific object.

        Args:
            bucket_name: Name of the bucket.
            object_name: Name of the object.
            provider: Bucket provider — "ais" (default), "aws", "gcp", etc.
        """
        client = get_client()
        obj = client.bucket(bucket_name, provider=provider).object(object_name)
        props = obj.head()
        return json.dumps(
            {
                "bucket": bucket_name,
                "object": object_name,
                "properties": dict(props.headers),
            },
            indent=2,
        )
