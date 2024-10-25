#
# Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import patch, Mock
from urllib3.util import Retry

from aistore.sdk import Client
from aistore.sdk.cluster import Cluster
from aistore.sdk.provider import Provider
from aistore.sdk.etl.etl import Etl
from aistore.sdk.request_client import RequestClient
from aistore.sdk.types import Namespace
from aistore.sdk.job import Job
from tests.const import ETL_NAME
from tests.utils import cases


class TestClient(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.endpoint = "https://aistore-endpoint"
        self.client = Client(self.endpoint)

    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_init_defaults(self, mock_request_client, mock_sm):
        Client(self.endpoint)
        mock_request_client.assert_called_with(
            endpoint=self.endpoint,
            session_manager=mock_sm.return_value,
            timeout=None,
            token=None,
        )

    @cases(
        (True, None, None, None, "dummy.token"),
        (False, "ca_cert_location", None, None, None),
        (False, None, 30.0, Retry(total=4), None),
        (False, None, (10, 30.0), Retry(total=5, connect=2), "dummy.token"),
    )
    @patch("aistore.sdk.client.SessionManager")
    @patch("aistore.sdk.client.RequestClient")
    def test_init(self, test_case, mock_request_client, mock_sm):
        skip_verify, ca_cert, timeout, retry, token = test_case
        Client(
            self.endpoint,
            skip_verify=skip_verify,
            ca_cert=ca_cert,
            timeout=timeout,
            retry=retry,
            token=token,
        )
        mock_sm.assert_called_with(
            retry=retry, ca_cert=ca_cert, skip_verify=skip_verify
        )
        mock_request_client.assert_called_with(
            endpoint=self.endpoint,
            session_manager=mock_sm.return_value,
            timeout=timeout,
            token=token,
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

        result = self.client.fetch_object_by_url(url)

        mock_bucket.assert_called_once_with(bck_name, provider=provider)
        mock_bucket_instance.object.assert_called_once_with(obj_name)
        self.assertEqual(result, expected_object)
