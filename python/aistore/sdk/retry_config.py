"""
Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
"""

import logging
from dataclasses import dataclass, field

from urllib3.util.retry import Retry
from urllib3.exceptions import TimeoutError as Urllib3TimeoutError
from tenacity import (
    Retrying,
    retry_if_exception_type,
    before_sleep_log,
    wait_random_exponential,
    stop_after_attempt,
)
from requests.exceptions import (
    ConnectTimeout,
    ReadTimeout,
    ChunkedEncodingError,
    ConnectionError as RequestsConnectionError,
)

from aistore.sdk.const import DEFAULT_COLD_GET_EST_BPS, DEFAULT_COLD_GET_MAX_WAIT
from aistore.sdk.errors import AISRetryableError

# Default Retry Exceptions
NETWORK_RETRY_EXCEPTIONS = (
    ConnectTimeout,
    ReadTimeout,
    ChunkedEncodingError,
    RequestsConnectionError,
    AISRetryableError,
    Urllib3TimeoutError,
)


def _log_and_raise_on_exhaust(retry_state):
    """
    `retry_error_callback`: log the underlying error with full traceback,
    then re-raise it. Per-retry attempts stay concise via `before_sleep_log`;
    the call stack is emitted once, here, when retries are exhausted.
    """
    exc = retry_state.outcome.exception()
    logging.getLogger().error("All retries exhausted; request failed", exc_info=exc)
    raise exc


@dataclass
class ColdGetConf:
    """
    Configuration class for retrying HEAD requests to objects that are not present in cluster when attempting a cold
    GET.

    **Attributes:**
        est_bandwidth_bps (int): Estimated bandwidth in bytes per second from the AIS cluster to backend buckets.
            Used to determine retry intervals for fetching remote objects.
            Raising this will decrease the initial time we expect object fetch to take.
            Defaults to 10 Gbps.
        max_cold_wait (int): Within an individual retry, the maximum seconds to wait for an object's write lock to be
            released before re-raising a ReadTimeoutError to be handled by the top-level RetryConfig.
            Defaults to 3 minutes.
        enable_remote_head (bool): Whether to send a HEAD request to the backend bucket if no size information for an
            object exists locally. Used for retry optimization, but increases requests to the remote backend.
            Defaults to True.
    """

    est_bandwidth_bps: int = field(default=DEFAULT_COLD_GET_EST_BPS)
    max_cold_wait: int = field(default=DEFAULT_COLD_GET_MAX_WAIT)
    enable_remote_head: bool = field(default=True)


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
        cold_get_conf (ColdGetConf): Configuration for retrying COLD GET requests, see ColdGetConf class.

    **Note on pickling (multi-process workloads):**
        `network_retry` is a tenacity `Retrying` object that internally uses
        lambdas/closures and is not picklable. When this config crosses a
        process boundary (e.g. PyTorch `DataLoader(num_workers > 0)` under the
        `forkserver`/`spawn` start method, Ray, Dask, `ProcessPoolExecutor`),
        `network_retry` is dropped during serialization and **rebuilt from
        `RetryConfig.default()` in the worker** — any caller-customized
        tenacity policy is lost in workers. Other fields (`http_retry`,
        `cold_get_conf`) survive pickling unchanged. Single-process usage is
        unaffected.
    """

    http_retry: Retry
    network_retry: Retrying
    cold_get_conf: ColdGetConf = field(default_factory=ColdGetConf)

    @staticmethod
    def default() -> "RetryConfig":
        """
        Returns the default retry configuration for AIStore.
        """
        return RetryConfig(
            http_retry=Retry(
                total=5,
                backoff_factor=3.0,  # 3s, 6s, 12s, 24s, 48s (total 93s)
                # NOTE: Status codes 429 (Too Many Requests) and 503 (Service Unavailable)
                # Distinguish whether the rate limit occurred at the frontend (client-facing)
                # or backend (cloud-facing).
                # - For backend rate-limiting, find and apply a better-suited config.
                #   See (`$ ais config cluster rate_limit --json`).
                # - For frontend rate-limiting, if the default config is not sufficient, you can adjust the
                #   `http_retry` configuration.
                status_forcelist=[429, 500, 502, 503, 504],
                connect=0,
                read=0,
            ),
            network_retry=Retrying(
                wait=wait_random_exponential(multiplier=1, min=1, max=30),
                stop=stop_after_attempt(7),
                retry=retry_if_exception_type(NETWORK_RETRY_EXCEPTIONS),
                before_sleep=before_sleep_log(logging.getLogger(), logging.WARNING),
                retry_error_callback=_log_and_raise_on_exhaust,
            ),
        )

    def __getstate__(self):
        # `network_retry` (tenacity Retrying) holds a lambda predicate and a
        # `before_sleep_log` closure — neither is picklable. Drop here and
        # rebuild with defaults in `__setstate__` so RetryConfig can cross
        # process boundaries — required for PyTorch DataLoader workers under
        # `forkserver` (POSIX default on Python 3.14+). User-customized
        # tenacity policy is replaced by the default in workers.
        state = self.__dict__.copy()
        state["network_retry"] = None
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.network_retry = RetryConfig.default().network_retry
