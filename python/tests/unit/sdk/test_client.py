#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest
import os
import warnings
from unittest.mock import patch, Mock
from urllib3.util import Retry

from tenacity import Retrying, stop_after_delay, wait_exponential
from aistore.sdk import Client
from aistore.sdk.cluster import Cluster
from aistore.sdk.provider import Provider
from aistore.sdk.etl.etl import Etl
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import Namespace
from aistore.sdk.job import Job
from aistore.sdk.retry_config import RetryConfig
from aistore.sdk.const import AIS_CONNECT_TIMEOUT, AIS_READ_TIMEOUT, AIS_MAX_CONN_POOL

from tests.const import ETL_NAME
from tests.utils import cases


class TestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.client = Client(self.endpoint)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    @patch("aistore.sdk.retry_config.RetryConfig.default")
    def test_init_defaults(self, mock_retry_config, mock_request_client, mock_sm):
        retry_config = RetryConfig.default()
        mock_retry_config.return_value = retry_config
        Client(self.endpoint)
        mock_sm.assert_called_with(
            retry=retry_config.http_retry,
            ca_cert=None,
            client_cert=None,
            skip_verify=False,
            max_pool_size=10,
        )
        mock_request_client.assert_called_with(
            endpoint=self.endpoint,
            session_manager=mock_sm.return_value,
            timeout=(3, 20),
            token=None,
            retry_config=retry_config,
        )

    @cases(
        (True, None, None, 0, None, None, "dummy.token", 50),
        (False, "ca_cert_location", None, 0, None, None, None, 10),
        (False, None, "client_cert_location", 0, None, None, None, None),
        (
            False,
            None,
            None,
            30.0,
            Retry(total=10),
            Retrying(stop=stop_after_delay(60)),
            None,
            100,
        ),
        (
            False,
            None,
            None,
            (10, 30.0),
            Retry(total=5, connect=2),
            Retrying(stop=stop_after_delay(60), wait=wait_exponential(multiplier=2)),
            "dummy.token",
            10,
        ),
    )
    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_init(self, test_case, mock_request_client, mock_sm):
        (
            skip_verify,
            ca_cert,
            client_cert,
            timeout,
            http_retry,
            network_retry,
            token,
            max_pool_size,
        ) = test_case
        retry_config = RetryConfig(http_retry=http_retry, network_retry=network_retry)
        Client(
            self.endpoint,
            skip_verify=skip_verify,
            ca_cert=ca_cert,
            client_cert=client_cert,
            timeout=timeout,
            retry_config=retry_config,
            token=token,
            max_pool_size=max_pool_size,
        )
        if timeout == 0:
            timeout = None
        if not max_pool_size:
            max_pool_size = 10
        mock_sm.assert_called_with(
            retry=http_retry,
            ca_cert=ca_cert,
            client_cert=client_cert,
            skip_verify=skip_verify,
            max_pool_size=max_pool_size,
        )
        mock_request_client.assert_called_with(
            endpoint=self.endpoint,
            session_manager=mock_sm.return_value,
            timeout=timeout,
            token=token,
            retry_config=retry_config,
        )

    @cases(*Provider)
    def test_bucket(self, provider):
        bck_name = "bucket_123"
        namespace = Namespace(uuid="id", name="namespace")
        bucket = self.client.bucket(bck_name, provider, namespace)
        self.assertIn(self.endpoint, bucket.client.base_url)
        self.assertIsInstance(bucket.client, RequestClient)
        self.assertEqual(bck_name, bucket.name)
        self.assertEqual(provider, bucket.provider)
        self.assertEqual(namespace, bucket.namespace)

    def test_cluster(self):
        res = self.client.cluster()
        self.assertIn(self.endpoint, res.client.base_url)
        self.assertIsInstance(res.client, RequestClient)
        self.assertIsInstance(res, Cluster)

    def test_job(self):
        job_id = "1234"
        job_kind = "test kind"
        res = self.client.job(job_id, job_kind)
        self.assertIsInstance(res, Job)
        self.assertEqual(job_id, res.job_id)
        self.assertEqual(job_kind, res.job_kind)

    def test_etl(self):
        res = self.client.etl(ETL_NAME)
        self.assertIsInstance(res, Etl)
        self.assertEqual(ETL_NAME, res.name)

    @patch("aistore.sdk.client.Client.bucket")
    @patch("aistore.sdk.utils.parse_url")
    def test_fetch_object_from_url(self, mock_parse_url, mock_bucket):
        url = "ais://bucket/object"
        provider = "ais"
        bck_name = "bucket"
        obj_name = "object"

        mock_parse_url.return_value = (provider, bck_name, obj_name)

        mock_bucket_instance = Mock()
        mock_bucket.return_value = mock_bucket_instance

        expected_object = Mock()
        mock_bucket_instance.object.return_value = expected_object

        result = self.client.get_object_from_url(url)

        mock_bucket.assert_called_once_with(bck_name, provider=provider)
        mock_bucket_instance.object.assert_called_once_with(obj_name)
        self.assertEqual(result, expected_object)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_env_vars_timeout(self, mock_request_client, mock_sm):
        """Test timeout from environment variables"""
        os.environ[AIS_CONNECT_TIMEOUT] = "5"
        os.environ[AIS_READ_TIMEOUT] = "30"
        try:
            Client(self.endpoint)
            mock_request_client.assert_called_once()
            mock_sm.assert_called_once()
            self.assertEqual(
                mock_request_client.call_args.kwargs["timeout"], (5.0, 30.0)
            )
        finally:
            os.environ.pop(AIS_CONNECT_TIMEOUT, None)
            os.environ.pop(AIS_READ_TIMEOUT, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_env_vars_max_pool(self, mock_request_client, mock_sm):
        """Test max_pool_size from environment variable"""
        os.environ[AIS_MAX_CONN_POOL] = "20"
        try:
            Client(self.endpoint)
            mock_sm.assert_called_once()
            mock_request_client.assert_called_once()
            self.assertEqual(mock_sm.call_args.kwargs["max_pool_size"], 20)
        finally:
            os.environ.pop(AIS_MAX_CONN_POOL, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_params_override_env_vars(self, mock_request_client, mock_sm):
        """Test that explicit params override env vars"""
        os.environ[AIS_CONNECT_TIMEOUT] = "5"
        os.environ[AIS_READ_TIMEOUT] = "30"
        os.environ[AIS_MAX_CONN_POOL] = "20"
        try:
            Client(self.endpoint, timeout=(10, 60), max_pool_size=50)
            self.assertEqual(mock_request_client.call_args.kwargs["timeout"], (10, 60))
            self.assertEqual(mock_sm.call_args.kwargs["max_pool_size"], 50)
        finally:
            os.environ.pop(AIS_CONNECT_TIMEOUT, None)
            os.environ.pop(AIS_READ_TIMEOUT, None)
            os.environ.pop(AIS_MAX_CONN_POOL, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_default_none_uses_env_vars(self, mock_request_client, mock_sm):
        """Test that default timeout=None checks env vars"""
        os.environ[AIS_CONNECT_TIMEOUT] = "5"
        os.environ[AIS_READ_TIMEOUT] = "30"
        try:
            # Not passing timeout (defaults to None) should use env vars
            Client(self.endpoint)
            self.assertEqual(
                mock_request_client.call_args.kwargs["timeout"], (5.0, 30.0)
            )
            mock_sm.assert_called_once()
        finally:
            os.environ.pop(AIS_CONNECT_TIMEOUT, None)
            os.environ.pop(AIS_READ_TIMEOUT, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_invalid_env_var_with_warning(self, mock_request_client, mock_sm):
        """Test that invalid env vars trigger warnings and use defaults"""
        os.environ[AIS_CONNECT_TIMEOUT] = "invalid"
        os.environ[AIS_MAX_CONN_POOL] = "not_a_number"
        try:
            with warnings.catch_warnings(record=True) as w:
                warnings.simplefilter("always")
                Client(self.endpoint)
                # Should have warnings for invalid values
                self.assertGreater(len(w), 0)
                # Should fall back to defaults
                self.assertEqual(mock_request_client.call_args.kwargs["timeout"][0], 3)
                self.assertEqual(mock_sm.call_args.kwargs["max_pool_size"], 10)
        finally:
            os.environ.pop(AIS_CONNECT_TIMEOUT, None)
            os.environ.pop(AIS_MAX_CONN_POOL, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_timeout_zero_converts_to_none(self, mock_request_client, mock_sm):
        """Test that timeout=0 disables timeout (converts to None)"""
        Client(self.endpoint, timeout=0)
        mock_request_client.assert_called_once()
        mock_sm.assert_called_once()
        self.assertIsNone(mock_request_client.call_args.kwargs["timeout"])

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_timeout_tuple_with_zero(self, mock_request_client, mock_sm):
        """Test that timeout=(0, 20) converts 0 to None for connect timeout"""
        Client(self.endpoint, timeout=(0, 20))
        mock_sm.assert_called_once()
        timeout = mock_request_client.call_args.kwargs["timeout"]
        self.assertEqual(timeout, (None, 20))

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_timeout_tuple_with_zero_read(self, mock_request_client, mock_sm):
        """Test that timeout=(5, 0) converts 0 to None for read timeout"""
        Client(self.endpoint, timeout=(5, 0))
        mock_sm.assert_called_once()
        timeout = mock_request_client.call_args.kwargs["timeout"]
        self.assertEqual(timeout, (5, None))

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_env_var_zero_for_connect_timeout(self, mock_request_client, mock_sm):
        """Test that AIS_CONNECT_TIMEOUT=0 sets None for connect timeout only"""
        os.environ[AIS_CONNECT_TIMEOUT] = "0"
        try:
            Client(self.endpoint)
            timeout = mock_request_client.call_args.kwargs["timeout"]
            self.assertEqual(
                timeout, (None, 20)
            )  # None for connect, 20 (default) for read
            mock_sm.assert_called_once()
        finally:
            os.environ.pop(AIS_CONNECT_TIMEOUT, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_env_var_zero_for_read_timeout(self, mock_request_client, mock_sm):
        """Test that AIS_READ_TIMEOUT=0 sets None for read timeout only"""
        os.environ[AIS_READ_TIMEOUT] = "0"
        try:
            Client(self.endpoint)
            timeout = mock_request_client.call_args.kwargs["timeout"]
            self.assertEqual(
                timeout, (3, None)
            )  # 3 (default) for connect, None for read
            mock_sm.assert_called_once()
        finally:
            os.environ.pop(AIS_READ_TIMEOUT, None)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_env_var_both_zero_disables_timeout(self, mock_request_client, mock_sm):
        """Test that both env vars=0 disables timeout completely"""
        os.environ[AIS_CONNECT_TIMEOUT] = "0"
        os.environ[AIS_READ_TIMEOUT] = "0"
        try:
            Client(self.endpoint)
            self.assertIsNone(mock_request_client.call_args.kwargs["timeout"])
            mock_sm.assert_called_once()
        finally:
            os.environ.pop(AIS_CONNECT_TIMEOUT, None)
            os.environ.pop(AIS_READ_TIMEOUT, None)
