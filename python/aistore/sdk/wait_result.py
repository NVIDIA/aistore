#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from dateutil.parser import isoparse

from aistore.sdk.types import JobSnap


@dataclass
class WaitResult:
    """
    Result of waiting for a job to reach a desired state.

    Attributes:
        job_id: The job ID
        success: True if the wait condition was met successfully
        error: Error message, if any
        end_time: When the job ended, if any
    """

    job_id: str
    success: bool
    error: Optional[str] = None
    end_time: Optional[datetime] = None

    def __bool__(self) -> bool:
        """Returns True if wait operation was successful."""
        return self.success

    @classmethod
    def from_snapshots(cls, job_id: str, snapshots: List[JobSnap]):
        """
        Create a WaitResult from a list of job snapshots.

        Args:
            job_id: The job ID
            snapshots: List of job snapshots from all target nodes

        Returns:
            WaitResult indicating success/failure based on snapshot states
        """
        aborted = False
        error_msg = None
        all_ended = True
        end_times = []

        for s in snapshots:
            if s.aborted:
                aborted = True
            if s.abort_err and error_msg is None:
                error_msg = s.abort_err
            if not (s.end_time and s.end_time != "0001-01-01T00:00:00Z"):
                all_ended = False
            elif s.end_time:
                end_times.append(isoparse(s.end_time))

        success = not aborted and not error_msg
        error = error_msg or ("aborted" if aborted else None)
        end_time = max(end_times) if all_ended and end_times else None

        return cls(job_id=job_id, success=success, error=error, end_time=end_time)
