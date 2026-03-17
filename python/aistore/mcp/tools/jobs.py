#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""Job and ETL management tools."""

import json
from typing import Callable

from mcp.server.fastmcp import FastMCP


def register_job_tools(
    mcp: FastMCP, get_client: Callable
):  # pylint: disable=too-many-statements
    """Register all job and ETL-related MCP tools."""

    @mcp.tool()
    def ais_list_jobs(kind: str = "") -> str:
        """List all jobs in the cluster with their status.

        Args:
            kind: Filter by job kind (e.g., "copy-bck", "etl", "rebalance").
                  Empty string means all jobs.
        """
        client = get_client()
        statuses = client.cluster().list_jobs_status(job_kind=kind)
        result = [
            {
                "id": s.uuid,
                "error": s.err or "",
                "finished": s.end_time != 0,
                "aborted": s.aborted,
            }
            for s in statuses
        ]
        return json.dumps(
            {"kind_filter": kind, "count": len(result), "jobs": result},
            indent=2,
        )

    @mcp.tool()
    def ais_running_jobs() -> str:
        """List currently running jobs."""
        client = get_client()
        running = client.cluster().list_running_jobs()
        return json.dumps(
            {"count": len(running), "jobs": running},
            indent=2,
        )

    @mcp.tool()
    def ais_job_status(job_id: str) -> str:
        """Get detailed status of a specific job.

        Args:
            job_id: The job ID to check.
        """
        client = get_client()
        job = client.job(job_id=job_id)
        status = job.status()
        result = {
            "id": job_id,
            "error": status.err or "",
            "finished": status.end_time != 0,
            "aborted": status.aborted,
        }

        # Try to get detailed snapshots
        try:
            snapshots = job.get_details().list_snapshots()
            result["snapshots"] = [_format_snapshot(snap) for snap in snapshots]
        except Exception as exc:  # pylint: disable=broad-except
            result["snapshots_error"] = str(exc)

        return json.dumps(result, indent=2, default=str)

    @mcp.tool()
    def ais_list_etls() -> str:
        """List all ETL transformers in the cluster."""
        client = get_client()
        etls = client.cluster().list_etls()
        result = [
            {
                "id": e.id,
                "xaction_id": getattr(e, "xaction_id", ""),
                "stage": getattr(e, "stage", ""),
                "obj_count": getattr(e, "obj_count", 0),
                "in_bytes": getattr(e, "in_bytes", 0),
                "out_bytes": getattr(e, "out_bytes", 0),
            }
            for e in etls
        ]
        return json.dumps(
            {"count": len(result), "etls": result},
            indent=2,
        )

    @mcp.tool()
    def ais_etl_details(etl_name: str, job_id: str = "") -> str:
        """Get ETL details including object-level errors.

        Args:
            etl_name: Name of the ETL.
            job_id: Optional job ID to get details for a specific run.
        """
        client = get_client()
        etl = client.etl(etl_name)
        result = {"name": etl_name}

        try:
            details = etl.view(job_id=job_id)
            if details.obj_errors:
                result["obj_errors"] = [
                    {
                        "obj_name": e.obj_name,
                        "message": e.msg,
                        "error_code": e.ecode,
                    }
                    for e in details.obj_errors
                ]
            else:
                result["obj_errors"] = []
        except Exception as exc:  # pylint: disable=broad-except
            # Pydantic validation can fail if ETL spec has empty env values etc.
            result["error"] = f"Could not parse ETL details: {exc}"

        return json.dumps(result, indent=2)

    @mcp.tool()
    def ais_etl_logs(etl_name: str, target_id: str = "") -> str:
        """Get logs from an ETL pod.

        Args:
            etl_name: Name of the ETL.
            target_id: Optional target node ID to get logs from a specific pod.
                       If empty, returns logs from all pods (last 200 lines each).
        """
        client = get_client()
        try:
            entries = client.etl(etl_name).logs(target_id=target_id)
            return _format_etl_logs(etl_name, entries)
        except Exception as exc:  # pylint: disable=broad-except
            return json.dumps(
                {"etl_name": etl_name, "error": str(exc)},
                indent=2,
            )


def _format_snapshot(snap):
    """Format a single job snapshot for JSON output."""
    info = {
        "id": snap.id,
        "kind": snap.kind,
        "start_time": str(snap.start_time) if snap.start_time else "",
        "end_time": str(snap.end_time) if snap.end_time else "",
        "aborted": snap.aborted,
        "is_idle": snap.is_idle,
    }
    if snap.stats:
        info["stats"] = {
            "objects": snap.stats.objects,
            "bytes": snap.stats.bytes,
            "out_objects": snap.stats.out_objects,
            "out_bytes": snap.stats.out_bytes,
            "in_objects": snap.stats.in_objects,
            "in_bytes": snap.stats.in_bytes,
        }
    return info


def _format_etl_logs(etl_name, entries):
    """Format ETL log entries from the SDK into JSON output."""
    result = {"etl_name": etl_name, "logs": {}}
    for entry in entries:
        log_lines = entry.logs.strip().split("\n")
        result["logs"][entry.target_id or "unknown"] = {
            "total_lines": len(log_lines),
            "last_200_lines": "\n".join(log_lines[-200:]),
        }
    return json.dumps(result, indent=2)
