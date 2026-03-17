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
from aistore.mcp.tools.jobs import (
    register_job_tools,
    _format_etl_logs,
    _format_snapshot,
)
from aistore.sdk.types import ETLNodeLogs


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


class TestJobTools(unittest.TestCase):
    """Test job and ETL-related MCP tools."""

    def setUp(self):
        os.environ["AIS_ENDPOINT"] = "http://localhost:8080"
        self.client = MagicMock()
        self.mcp = FastMCP("test")
        register_job_tools(self.mcp, self._get_client)
        self.tools = {t.name: t for t in self.mcp._tool_manager.list_tools()}

    def _get_client(self):
        return self.client

    def test_tools_registered(self):
        expected = {
            "ais_list_jobs",
            "ais_running_jobs",
            "ais_job_status",
            "ais_list_etls",
            "ais_etl_details",
            "ais_etl_logs",
        }
        self.assertEqual(set(self.tools.keys()), expected)

    def test_list_jobs(self):
        status = MagicMock()
        status.uuid = "job-123"
        status.err = ""
        status.end_time = 0
        status.aborted = False
        self.client.cluster.return_value.list_jobs_status.return_value = [status]

        result = json.loads(self.tools["ais_list_jobs"].fn())
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["jobs"][0]["id"], "job-123")
        self.assertFalse(result["jobs"][0]["finished"])

    def test_running_jobs(self):
        self.client.cluster.return_value.list_running_jobs.return_value = [
            "etl-inline[etl-abc]"
        ]
        result = json.loads(self.tools["ais_running_jobs"].fn())
        self.assertEqual(result["count"], 1)

    def test_list_etls(self):
        etl = MagicMock()
        etl.id = "my-etl"
        etl.xaction_id = "etl-xyz"
        etl.stage = "Running"
        etl.obj_count = 100
        etl.in_bytes = 0
        etl.out_bytes = 0
        self.client.cluster.return_value.list_etls.return_value = [etl]

        result = json.loads(self.tools["ais_list_etls"].fn())
        self.assertEqual(result["count"], 1)
        self.assertEqual(result["etls"][0]["id"], "my-etl")

    def test_etl_details_with_pydantic_error(self):
        self.client.etl.return_value.view.side_effect = Exception("validation error")
        result = json.loads(self.tools["ais_etl_details"].fn(etl_name="bad-etl"))
        self.assertIn("error", result)
        self.assertIn("validation error", result["error"])

    def test_etl_logs_success(self):
        entry = ETLNodeLogs(
            target_id="t1", logs="INFO: server started\nERROR: something failed\n"
        )
        self.client.etl.return_value.logs.return_value = [entry]

        result = json.loads(self.tools["ais_etl_logs"].fn(etl_name="my-etl"))
        self.assertEqual(result["logs"]["t1"]["total_lines"], 2)
        self.assertIn("server started", result["logs"]["t1"]["last_200_lines"])

    def test_etl_logs_error(self):
        self.client.etl.return_value.logs.side_effect = Exception("connection refused")
        result = json.loads(self.tools["ais_etl_logs"].fn(etl_name="bad-etl"))
        self.assertIn("error", result)
        self.assertIn("connection refused", result["error"])


class TestFormatEtlLogs(unittest.TestCase):
    """Test the _format_etl_logs helper."""

    def test_formats_entries(self):
        entries = [
            ETLNodeLogs(target_id="t1", logs="line1\nline2\n"),
            ETLNodeLogs(target_id="t2", logs="line3\n"),
        ]
        result = json.loads(_format_etl_logs("test-etl", entries))
        self.assertEqual(result["logs"]["t1"]["total_lines"], 2)
        self.assertEqual(result["logs"]["t2"]["total_lines"], 1)

    def test_empty_entries(self):
        result = json.loads(_format_etl_logs("test-etl", []))
        self.assertEqual(result["logs"], {})


class TestFormatSnapshot(unittest.TestCase):
    """Test the _format_snapshot helper."""

    def test_with_stats(self):
        snap = MagicMock()
        snap.id = "job-1"
        snap.kind = "copy"
        snap.start_time = "2026-01-01T00:00:00"
        snap.end_time = ""
        snap.aborted = False
        snap.is_idle = False
        snap.stats.objects = 100
        snap.stats.bytes = 1024
        snap.stats.out_objects = 50
        snap.stats.out_bytes = 512
        snap.stats.in_objects = 50
        snap.stats.in_bytes = 512

        result = _format_snapshot(snap)
        self.assertEqual(result["id"], "job-1")
        self.assertEqual(result["stats"]["objects"], 100)

    def test_without_stats(self):
        snap = MagicMock()
        snap.id = "job-2"
        snap.kind = "etl"
        snap.start_time = None
        snap.end_time = None
        snap.aborted = True
        snap.is_idle = True
        snap.stats = None

        result = _format_snapshot(snap)
        self.assertEqual(result["id"], "job-2")
        self.assertTrue(result["aborted"])
        self.assertNotIn("stats", result)
