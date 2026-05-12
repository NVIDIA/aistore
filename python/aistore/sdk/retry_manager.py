#
# Copyright (c) 2023-2026, NVIDIA CORPORATION. All rights reserved.
#
from typing import Callable, Optional

import requests
import tenacity
from requests import PreparedRequest, Response

from aistore.sdk.const import HTTP_METHOD_GET, URL_PATH_OBJECTS
from aistore.sdk.lock_poller import LockPoller
from aistore.sdk.request_executor import RequestExecutor
from aistore.sdk.retry_config import RetryConfig
from aistore.sdk import utils

logger = utils.get_logger(__name__)


# pylint: disable=too-few-public-methods
class RetryManager:
    """
    Manages applying the retry configuration along with any custom logic to requests.

    Args:
        executor (RequestExecutor): Sends requests without a retry wrapper to the configured AIS endpoint
        retry_config (Optional[RetryConfig]): Retry configuration, containing config for both tenacity retry logic
            and logic for cold gets from remote backend providers.
    """

    def __init__(
        self,
        executor: RequestExecutor,
        retry_config: Optional[RetryConfig] = None,
    ):
        self._retry_config = retry_config or RetryConfig.default()
        self._lock_poller = LockPoller(executor, self._retry_config.cold_get_conf)
        self._retrying = self._build_retrying()

    def _build_retrying(self) -> tenacity.Retrying:
        """Compose `network_retry` with the cold-get write-lock polling hook."""
        retrying = self._retry_config.network_retry.copy()
        # Preserve existing or user-provided before_sleep
        existing_before_sleep = retrying.before_sleep
        if existing_before_sleep:

            def combined_before_sleep(*args):
                self._before_sleep(*args)
                existing_before_sleep(*args)

            retrying.before_sleep = combined_before_sleep
        else:
            retrying.before_sleep = self._before_sleep
        return retrying

    def __getstate__(self):
        # `_retrying` wraps a non-picklable closure; rebuild on unpickle.
        # See `RetryConfig.__getstate__` for the Py 3.14+ context.
        state = self.__dict__.copy()
        state["_retrying"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._retrying = self._build_retrying()

    @property
    def retry_config(self) -> RetryConfig:
        """Return the retry config provided to this manager"""
        return self._retry_config

    def _before_sleep(self, retry_state: tenacity.RetryCallState):
        """
        Hook called by Tenacity before sleeping between retries.
        Determine if we want to delay the retry.
        If so, use LockPoller to poll until the object is unlocked.

        Args:
            retry_state (tenacity.RetryCallState): Retry state from tenacity.
        """
        outcome = retry_state.outcome
        if not outcome:
            return
        exc = outcome.exception()

        # Early return if no special handling is needed for this exception
        if not isinstance(exc, requests.ConnectionError):
            return

        # Invalid case to delay polling: no usable PreparedRequest on the exception.
        req = exc.request
        if not isinstance(req, PreparedRequest):
            return

        # Invalid case to delay polling: not a remote-bucket cold-GET.
        if not self._should_delay_retry(exc, req):
            return

        # Otherwise: poll for in-flight cold-GET write-lock release
        logger.info(
            "Delaying retry by polling for object write-lock release after: '%s'",
            type(exc).__name__,
        )
        try:
            self._lock_poller.wait_for_unlock(req)
        except Exception as retry_err:
            # Allow tenacity to continue retrying and do not re-raise
            logger.error("Error while polling for object write-lock: %s", retry_err)

    @staticmethod
    def _should_delay_retry(
        exc: requests.ConnectionError, req: PreparedRequest
    ) -> bool:
        """
        If we get a read timeout, it's possible AIS is currently downloading and writing a remote object,
            so our initial request failed to acquire a read lock.

        Retrying immediately means we expect AIS to be finished downloading from remote,
            which may take longer than our default retry.

        Args:
            exc (requests.ConnectionError): Original exception.
            req (PreparedRequest): Validated request that produced `exc`.
        Returns:
            Whether to delay tenacity retry.
        """
        if not utils.is_read_timeout(exc):
            return False
        exc_type = type(exc).__name__
        # Do nothing if it's not a GET
        if req.method is None or req.method.lower() != HTTP_METHOD_GET:
            logger.debug("Received error: '%s' from non-GET request", exc_type)
            return False
        # Do nothing if it's not a request for an object
        if not req.url or f"{URL_PATH_OBJECTS}/" not in req.url:
            logger.debug("Received error: '%s' from non-object request", exc_type)
            return False
        # Do nothing if the bucket is not remote
        provider = utils.get_provider_from_request(req)
        if not provider.is_remote():
            logger.debug(
                "Received error: '%s' from object request to AIS bucket", exc_type
            )
            return False
        logger.debug(
            "Received error: '%s' from object GET request to bucket with provider '%s'",
            exc_type,
            provider.value,
        )
        return True

    def with_retry(
        self, request_func: Callable[..., Response], *args, **kwargs
    ) -> Response:
        """
        **Why `tenacity.retry` over `urllib3.Retry`?**
        `urllib3.Retry` always retries the **same failing URL**, which is problematic if a target is down or restarting.
        Instead, we retry at this stage with tenacity to re-create the initial request to the **proxy URL**.
        This request will then get redirected to the latest selected target.

        Args:
            request_func (Callable): Function that makes a request to AIS and returns a requests.Response object.
            *args: Args for request_func.
            **kwargs: Kwargs for request_func.

        Returns:
            Response object from the provided request_func.
        """
        return self._retrying(request_func, *args, **kwargs)
