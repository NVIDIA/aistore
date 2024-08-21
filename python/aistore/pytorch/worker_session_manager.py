"""
Worker Supported Request Client for PyTorch

This client allows Pytorch workers to have separate request sessions per thread
which is needed in order to use workers in a DataLoader as
the default implementation of RequestClient and requests is not thread-safe.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from torch.utils.data import get_worker_info

from aistore.sdk.session_manager import SessionManager


class WorkerSessionManager(SessionManager):
    """
    Extension that supports Pytorch and multiple workers of internal client for
    buckets, objects, jobs, etc. to use for making requests to an AIS cluster.

    Args:
        session_manager (SessionManager): Existing SessionManager to replace
    """

    def __init__(self, session_manager: SessionManager):
        super().__init__(
            retry=session_manager.retry,
            ca_cert=session_manager.ca_cert,
            skip_verify=session_manager.skip_verify,
        )
        self._worker_sessions = {}

    @property
    def session(self):
        """
        Returns an active request session acquired for a specific Pytorch dataloader worker.
        """
        # sessions are not thread safe, so we must return different sessions for each worker
        worker_info = get_worker_info()
        if worker_info is None:
            if self._session is None:
                self._session = self._create_session()
            return self._session
        # if we only have one session but multiple workers, create more
        if worker_info.id not in self._worker_sessions:
            self._worker_sessions[worker_info.id] = self._create_session()
        return self._worker_sessions[worker_info.id]
