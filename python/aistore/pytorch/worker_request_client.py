"""
Worker Supported Request Client for PyTorch

This client allows Pytorch workers to have separate request sessions per thread
which is needed in order to use workers in a DataLoader as
the default implementation of RequestClient and requests is not thread-safe.

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.sdk.request_client import RequestClient
from torch.utils.data import get_worker_info


class WorkerRequestClient(RequestClient):
    """
    Extension that supports Pytorch and multiple workers of internal client for
    buckets, objects, jobs, etc. to use for making requests to an AIS cluster.

    Args:
        client (RequestClient): Existing RequestClient to replace
    """

    def __init__(self, client: RequestClient):
        super().__init__(
            endpoint=client._endpoint,
            skip_verify=client._skip_verify,
            ca_cert=client._ca_cert,
            timeout=client._timeout,
            retry=client._retry,
            token=client._token,
        )
        self._worker_sessions = {}

    @property
    def session(self):
        """
        Returns: Active request session acquired for a specific Pytorch dataloader worker
        """
        # sessions are not thread safe, so we must return different sessions for each worker
        worker_info = get_worker_info()
        if worker_info is None:
            return self._session
        # if we only have one session but multiple workers, create more
        if worker_info.id not in self._worker_sessions:
            self._worker_sessions[worker_info.id] = self._create_new_session()
        return self._worker_sessions[worker_info.id]
