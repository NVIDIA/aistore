"""
Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
"""

import logging
from dataclasses import dataclass
from urllib3.util.retry import Retry
from tenacity import (
    Retrying,
    wait_exponential,
    stop_after_delay,
    retry_if_exception_type,
    before_sleep_log,
)
from requests.exceptions import (
    ConnectTimeout,
    ReadTimeout,
    ChunkedEncodingError,
    ConnectionError as RequestsConnectionError,
)
from aistore.sdk.errors import AISRetryableError

# Default Retry Exceptions
NETWORK_RETRY_EXCEPTIONS = (
    ConnectTimeout,
    ReadTimeout,
    ChunkedEncodingError,
    RequestsConnectionError,
    AISRetryableError,
)


@dataclass
class RetryConfig:
    """
    Configuration class for managing both HTTP and network retries in AIStore.

    AIStore implements two types of retries to ensure reliability and fault tolerance:

    1. **HTTP Retry (urllib3.Retry)** - Handles HTTP errors based on status codes (e.g., 429, 500, 502, 503, 504).
    2. **Network Retry (tenacity)** - Recovers from connection failures, timeouts, and unreachable targets.

    **Why two types of retries?**
    - AIStore uses **redirects** for GET/PUT operations.
    - If a target node is down, we must retry the request via the proxy instead of the same failing target.
    - `network_retry` ensures that the request is reattempted at the **proxy level**, preventing unnecessary failures.

    **Attributes:**
        http_retry (urllib3.Retry): Defines retry behavior for transient HTTP errors.
        network_retry (tenacity.Retrying): Configured `tenacity.Retrying` instance managing retries for network-related
            issues, such as connection failures, timeouts, or unreachable targets.
    """

    http_retry: Retry
    network_retry: Retrying

    @staticmethod
    def default() -> "RetryConfig":
        """
        Returns the default retry configuration for AIStore.
        """
        return RetryConfig(
            http_retry=Retry(
                total=3,
                backoff_factor=0.5,
                # NOTE: Status codes 429 (Too Many Requests) and 503 (Service Unavailable)
                # are temporarily handled here to mitigate rate-limiting issues.
                # This logic is intended for AIStore versions prior to 3.28 and should be removed afterward.
                # See: https://aistore.nvidia.com/blog/2025/03/19/rate-limit-blog
                # TODO (post-3.28):
                # Distinguish whether the rate limit occurred at the frontend (client-facing) or backend (cloud-facing).
                # - For backend rate-limiting, find and apply a better-suited config.
                #   See (`$ ais config cluster rate_limit --json`).
                # - For frontend rate-limiting, we need a smarter handling mechanism.
                #   The current HTTP retry strategy makes 3 attempts with 0.5s backoff,
                #   which may not be appropriate in all scenarios.
                status_forcelist=[429, 500, 502, 503, 504],
                connect=0,
                read=0,
            ),
            network_retry=Retrying(
                wait=wait_exponential(multiplier=1, min=1, max=10),
                stop=stop_after_delay(60),
                retry=retry_if_exception_type(NETWORK_RETRY_EXCEPTIONS),
                before_sleep=before_sleep_log(logging.getLogger(), logging.DEBUG),
                reraise=True,
            ),
        )
