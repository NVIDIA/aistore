#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
#
from typing import Callable, Optional

import requests
import tenacity
from requests import Response

from aistore.sdk.retry_config import RetryConfig
from aistore.sdk.const import HTTP_METHOD_GET, URL_PATH_OBJECTS
from aistore.sdk.presence_poller import PresencePoller
from aistore.sdk.provider import Provider
from aistore.sdk import utils

logger = utils.get_logger(__name__)


# pylint: disable=too-few-public-methods
class RetryManager:
    """
    Manages applying the retry configuration along with any custom logic to requests.

    Args:
        request_func (Callable): A function that takes request parameters and returns a requests.Response object.
        retry_config (Optional[RetryConfig]): Retry configuration, containing config for both tenacity retry logic
            and logic for cold gets from remote backend providers.
    """

    def __init__(
        self,
        request_func: Callable[..., Response],
        retry_config: Optional[RetryConfig] = None,
    ):
        self._retry_config = retry_config or RetryConfig.default()
        self._presence_poller = PresencePoller(
            request_func, self._retry_config.cold_get_conf
        )
        self._retrying = self._retry_config.network_retry.copy()
        # In case the provided Retrying instance already has a before_sleep, append our custom one first
        if (
            retry_config
            and retry_config.network_retry
            and retry_config.network_retry.before_sleep
        ):

            def combined_before_sleep(*args):
                self._before_sleep(*args)
                retry_config.network_retry.before_sleep(*args)

            self._retrying.before_sleep = combined_before_sleep
        else:
            self._retrying.before_sleep = self._before_sleep

    @property
    def retry_config(self) -> RetryConfig:
        """Return the retry config provided to this manager"""
        return self._retry_config

    def _before_sleep(self, retry_state: tenacity.RetryCallState):
        """
        Hook called by Tenacity before sleeping between retries.
        Determine if we want to delay the retry.
        If so, use PresencePoller to poll until the object is present.

        Args:
            retry_state (tenacity.RetryCallState): Retry state from tenacity.
        """
        outcome = retry_state.outcome
        if not outcome:
            return
        exc = retry_state.outcome.exception()

        # Early return if no special handling is needed
        if not isinstance(exc, requests.ConnectionError):
            return
        if not self._should_delay_retry(exc):
            return
        # Otherwise: poll for object presence
        logger.info(
            "Delaying retry by polling for object presence after: '%s'",
            type(exc).__name__,
        )
        try:
            self._presence_poller.wait_for_presence(exc.request)
        except Exception as retry_err:
            # Allow tenacity to continue retrying and do not re-raise
            logger.error("Error while polling for object presence: %s", retry_err)

    @staticmethod
    def _should_delay_retry(exc: requests.ConnectionError) -> bool:
        """
        If we get a read timeout, it's possible AIS is currently downloading and writing a remote object,
            so our initial request failed to acquire a read lock.

        Retrying immediately means we expect AIS to be finished downloading from remote,
            which may take longer than our default retry.

        Args:
            exc (requests.ConnectionError): Original exception.
        Returns:
            Whether to delay tenacity retry.
        """
        if not utils.is_read_timeout(exc):
            return False
        if exc.request is None:
            return False
        req = exc.request
        exc_type = type(exc).__name__
        # Do nothing if it's not a GET
        if req.method is None or req.method.lower() != HTTP_METHOD_GET:
            logger.debug("Received error: '%s' from non-GET request", exc_type)
            return False
        # Do nothing if it's not a request for an object
        if f"{URL_PATH_OBJECTS}/" not in req.url:
            logger.debug("Received error: '%s' from non-object request", exc_type)
            return False
        # Do nothing if the bucket is not remote
        provider = utils.get_provider_from_request(req)
        if provider == Provider.AIS:
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
