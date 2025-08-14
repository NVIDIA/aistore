#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
from typing import Callable

from requests import PreparedRequest, Response
from tenacity import Retrying, wait_exponential, retry_if_not_result, stop_after_delay

from aistore.sdk.const import HTTP_METHOD_HEAD
from aistore.sdk.obj.object_attributes import ObjectAttributes
from aistore.sdk.retry_config import ColdGetConf
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


# pylint: disable=too-few-public-methods
class PresencePoller:
    """
    Utility class for making a best-effort attempt at retrying HEAD calls to an object until it is present in
    cluster.

    This class provides a way to delay this retry until the object is present.
    """

    def __init__(
        self,
        session_request_fn: Callable[..., Response],
        cold_get_conf: ColdGetConf,
    ):
        self._session_request_fn = session_request_fn
        self._cold_get_conf = cold_get_conf

    def wait_for_presence(self, req: PreparedRequest):
        """
        Calls HEAD on the object from the given PreparedRequest.
        Uses the object attributes to make an intelligent guess at when we should re-raise the
         error and proceed to the next retry, based on the size of the object.
        This method has no effect besides delaying a higher-level function until presence is confirmed or
         retries are exhausted.

        Args:
            req (PreparedRequest): Request made to AIS target before receiving initial error.
        """
        # Repeat the same initial request, but with HEAD instead of GET
        obj_attr = self._head_req(req)
        # If the object is now present, retry should succeed, so return immediately
        if obj_attr.present:
            return
        # Otherwise, use an adjusted retry config based on the object size
        retryer = self._create_cold_get_retryer(obj_attr.size)
        logger.debug("Retrying HEAD calls until object is present")
        retryer(self._head_req, req)

    def _create_cold_get_retryer(self, size: int) -> Retrying:
        """
        Given an object size, create a retryer.
        Args:
            size (int) : Object size in bytes.
        Returns: tenacity.Retrying determining how to retry the given function.

        """
        expected_time = size / self._cold_get_conf.est_bandwidth_bps
        # Based on expected time, with ceiling of half the total max time
        longest_sleep = min(
            int(self._cold_get_conf.max_cold_wait / 2), int(expected_time * 4)
        )
        # Based on expected, with floor of 1 and ceiling of 10
        shortest_sleep = min(max(expected_time / 4, 2), 10)
        return Retrying(
            wait=wait_exponential(multiplier=2, min=shortest_sleep, max=longest_sleep),
            stop=stop_after_delay(self._cold_get_conf.max_cold_wait),
            retry=retry_if_not_result(lambda p: p.present),
        )

    def _head_req(self, req: PreparedRequest) -> ObjectAttributes:
        """
        Given a PreparedRequest, send a HEAD to that same object URL.
        Args:
            req (PreparedRequest): Initial request for an object.

        Returns: Object attributes of the object in the initial request.
        """
        return ObjectAttributes(
            self._session_request_fn(HTTP_METHOD_HEAD, req.url, req.headers).headers
        )
