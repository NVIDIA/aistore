#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-function-docstring,protected-access

"""Unit tests for AIStore MCP server tools."""

import json
import os
import unittest
from unittest.mock import MagicMock

from mcp.server.fastmcp import FastMCP

from aistore.mcp.tools.cluster import register_cluster_tools
from aistore.mcp.tools.buckets import register_bucket_tools


class TestClusterTools(unittest.TestCase):
    """Test cluster-related MCP tools."""

    def setUp(self):
        os.environ["AIS_ENDPOINT"] = "http://localhost:8080"
        self.client = MagicMock()
        self.mcp = FastMCP("test")
        register_cluster_tools(self.mcp, self._get_client)
        self.tools = {t.name: t for t in self.mcp._tool_manager.list_tools()}

    def _get_client(self):
        return self.client

    def test_tools_registered(self):
        expected = {
            "ais_cluster_health",
            "ais_cluster_info",
            "ais_cluster_performance",
            "ais_is_ready",
        }
        self.assertEqual(set(self.tools.keys()), expected)

    def test_cluster_health_healthy(self):
        cluster = self.client.cluster.return_value
        cluster.is_ready.return_value = True

        node = MagicMock()
        node.daemon_id = "t1"
        node.public_net.direct_url = "http://t1:8080"

        proxy = MagicMock()
        proxy.daemon_id = "p1"
        proxy.public_net.direct_url = "http://p1:8080"

        info = MagicMock()
        info.uuid = "test-uuid"
        info.version = 5
        info.tmap = {"t1": node}
        info.pmap = {"p1": proxy}
        info.proxy_si = proxy
        cluster.get_info.return_value = info

        result = json.loads(self.tools["ais_cluster_health"].fn())
        self.assertEqual(result["status"], "healthy")
        self.assertTrue(result["ready"])
        self.assertEqual(result["targets"]["count"], 1)
        self.assertEqual(result["proxies"]["count"], 1)

    def test_cluster_health_unhealthy(self):
        cluster = self.client.cluster.return_value
        cluster.is_ready.return_value = False
        info = MagicMock()
        info.uuid = "uuid"
        info.version = 1
        info.tmap = {}
        info.pmap = {}
        info.proxy_si = None
        cluster.get_info.return_value = info

        result = json.loads(self.tools["ais_cluster_health"].fn())
        self.assertEqual(result["status"], "unhealthy")
        self.assertFalse(result["ready"])

    def test_is_ready_success(self):
        self.client.cluster.return_value.is_ready.return_value = True
        result = json.loads(self.tools["ais_is_ready"].fn())
        self.assertTrue(result["ready"])

    def test_is_ready_failure(self):
        self.client.cluster.return_value.is_ready.side_effect = ConnectionError(
            "refused"
        )
        result = json.loads(self.tools["ais_is_ready"].fn())
        self.assertFalse(result["ready"])
        self.assertIn("refused", result["error"])


class TestBucketTools(unittest.TestCase):
    """Test bucket-related MCP tools."""

    def setUp(self):
        self.client = MagicMock()
        self.mcp = FastMCP("test")
        register_bucket_tools(self.mcp, self._get_client)
        self.tools = {t.name: t for t in self.mcp._tool_manager.list_tools()}

    def _get_client(self):
        return self.client

    def test_tools_registered(self):
        expected = {
            "ais_list_buckets",
            "ais_bucket_summary",
            "ais_list_objects",
            "ais_object_info",
        }
        self.assertEqual(set(self.tools.keys()), expected)

    def test_list_buckets(self):
        bck1 = MagicMock()
        bck1.name = "bucket1"
        bck1.provider = "ais"
        bck2 = MagicMock()
        bck2.name = "bucket2"
        bck2.provider = "ais"
        self.client.cluster.return_value.list_buckets.return_value = [bck1, bck2]

        result = json.loads(self.tools["ais_list_buckets"].fn())
        self.assertEqual(result["count"], 2)
        self.assertEqual(result["buckets"][0]["name"], "bucket1")

    def test_list_objects(self):
        entry = MagicMock()
        entry.name = "obj1.txt"
        entry.size = 1024
        entries = MagicMock()
        entries.entries = [entry]
        self.client.bucket.return_value.list_objects.return_value = entries

        result = json.loads(
            self.tools["ais_list_objects"].fn(bucket_name="test", limit=10)
        )
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["objects"][0]["name"], "obj1.txt")
        self.assertEqual(result["objects"][0]["size"], 1024)
