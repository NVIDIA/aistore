#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#
from typing import Dict

import requests
from requests import PreparedRequest
from tenacity import (
    Retrying,
    wait_exponential,
    retry_if_result,
    stop_after_delay,
)

from aistore.sdk import utils
from aistore.sdk.const import (
    ACT_CHECK_LOCK,
    HTTP_METHOD_HEAD,
    HTTP_METHOD_POST,
    QPARAM_PROVIDER,
    STATUS_LOCKED,
    QPARAM_NAMESPACE,
    DEFAULT_COLD_GET_EST_BPS,
    STATUS_OK,
)
from aistore.sdk.obj.object_attributes import ObjectAttributes
from aistore.sdk.request_executor import RequestExecutor
from aistore.sdk.retry_config import ColdGetConf
from aistore.sdk.types import ActionMsg
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)

MIN_WAIT_FLOOR = 2
MIN_WAIT_CEILING = 10
# How much to scale wait times between attempts based on object size and expected bandwidth.
# A factor of one uses expected time directly, while larger values start lower and scale higher.
OBJ_SIZE_SCALING_FACTOR = 4


# pylint: disable=too-few-public-methods
class LockPoller:
    """
    Best-effort delay for tenacity retries on read-timeouts during a cold GET.

    When a remote-bucket GET request times out, the most likely cause is that an
    in-flight cold-get is holding the object's write lock while it streams
    from the remote backend (e.g., s3).
    This poller asks the cluster whether the object is currently write-locked.
    If locked, it polls check-lock with a size-scaled exponential backoff
    until the object is unlocked or `max_cold_wait` expires.

    This routes all polling requests to the original configured AIStore endpoint for the provided RequestExecutor.

    Args:
        executor (RequestExecutor): Pre-configured executor used for sending requests to AIS
        cold_get_conf (ColdGetConf): Configuration determining how to poll AIS for cold-get status
    """

    def __init__(
        self,
        executor: RequestExecutor,
        cold_get_conf: ColdGetConf,
    ):
        self._executor = executor
        self._cold_get_conf = cold_get_conf

    def wait_for_unlock(self, req: PreparedRequest):
        """
        Delay until the object referenced by the failed request is no longer
        write-locked (i.e. any in-flight cold-get has released its lock).

        Args:
            req (PreparedRequest): Request made to AIS target before
                receiving the initial error.
        """
        # Preserve only qparams needed to regenerate the request
        path, params = utils.get_object_url_components(
            req, [QPARAM_PROVIDER, QPARAM_NAMESPACE]
        )
        if not self._is_write_locked(path, params):
            return
        # Size-scaled backoff: an upfront HEAD gives us the object size so we
        # can pace the poll based on the configured estimated bandwidth. A
        # HEAD failure here just means we fall back to size=0 (i.e. minimum
        # backoff bounds); we still want to poll the lock.
        size = self._head_size(path, params)
        retryer = self._create_cold_get_retryer(size)
        logger.debug(
            "Polling check-lock for expected size '%s' bytes until object at '%s' is unlocked",
            size,
            path,
        )
        retryer(self._is_write_locked, path, params)

    def _create_cold_get_retryer(self, size: int) -> Retrying:
        """
        Build a tenacity `Retrying` that re-calls a bool-returning function
        as long as it keeps returning True (i.e. the object stays locked),
        with a size-scaled exponential backoff capped at `max_cold_wait`.
        """
        bw_conf = self._cold_get_conf.est_bandwidth_bps
        bw = bw_conf if bw_conf > 0 else DEFAULT_COLD_GET_EST_BPS
        expected_time = size / bw
        # Based on expected, with floor MIN_WAIT_FLOOR and ceiling MIN_WAIT_CEILING
        shortest_sleep = min(
            max(expected_time / OBJ_SIZE_SCALING_FACTOR, MIN_WAIT_FLOOR),
            MIN_WAIT_CEILING,
        )
        # Based on expected time, with ceiling of half the total max time
        longest_sleep = max(
            shortest_sleep,
            min(
                int(self._cold_get_conf.max_cold_wait / 2),
                int(expected_time * OBJ_SIZE_SCALING_FACTOR),
            ),
        )
        return Retrying(
            wait=wait_exponential(multiplier=2, min=shortest_sleep, max=longest_sleep),
            stop=stop_after_delay(self._cold_get_conf.max_cold_wait),
            retry=retry_if_result(lambda locked: locked),
        )

    def _is_write_locked(self, path: str, params: Dict[str, str]) -> bool:
        """
        POST check-lock via the configured RequestExecutor.

        Returns: True iff the target replied 423 (write-locked). False on
        any other status or on transport error; the caller will then stop
        delaying and let the outer tenacity retry continue.
        """
        try:
            resp = self._executor.request(
                HTTP_METHOD_POST,
                path,
                params=params,
                data=ActionMsg(action=ACT_CHECK_LOCK).model_dump_json(),
            )
        except requests.RequestException as e:
            logger.warning(
                "check-lock call failed (%s); breaking from lock polling to return to outer handling",
                e,
                exc_info=True,
            )
            return False
        return resp.status_code == STATUS_LOCKED

    def _head_size(self, path: str, params: Dict[str, str]) -> int:
        """
        Fresh HEAD via the configured RequestExecutor to retrieve the object
        size for scaling the poll backoff. Returns 0 on any failure (so the
        poller falls back to the minimum backoff bounds).

        HEAD on a cold-get with write-lock held should fail to load local object metadata.
        It will fall back to previous cached attributes if the object exists (lock is a new update).
        If the object doesn't exist locally, it will call HEAD on the remote backend object.
        """
        try:
            resp = self._executor.request(HTTP_METHOD_HEAD, path, params=params)
        except requests.RequestException as e:
            logger.debug("HEAD for cold-get sizing failed (%s); using size=0", e)
            return 0
        if resp.status_code != STATUS_OK:
            logger.debug(
                "HEAD for cold-get sizing returned %d; using size=0",
                resp.status_code,
            )
            return 0
        return ObjectAttributes(resp.headers).size
