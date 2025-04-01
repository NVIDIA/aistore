#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
import logging

from tenacity import (
    Retrying,
    retry_if_exception_type,
    before_sleep_log,
    stop_before_delay,
    wait_fixed,
)

from aistore.sdk.retry_config import RetryConfig, NETWORK_RETRY_EXCEPTIONS
from aistore.sdk.utils import get_logger
from aistore.client import Client
from tests.const import TEST_TIMEOUT
from tests.integration import CLUSTER_ENDPOINT

test_retrying = Retrying(
    wait=wait_fixed(1),
    stop=stop_before_delay(5),
    retry=retry_if_exception_type(NETWORK_RETRY_EXCEPTIONS),
    before_sleep=before_sleep_log(get_logger(__name__), logging.DEBUG),
    reraise=True,
)
TEST_RETRY_CONFIG = RetryConfig(RetryConfig.default().http_retry, test_retrying)
DEFAULT_TEST_CLIENT = Client(
    CLUSTER_ENDPOINT,
    retry_config=TEST_RETRY_CONFIG,
    timeout=(5, TEST_TIMEOUT - 2),
)
