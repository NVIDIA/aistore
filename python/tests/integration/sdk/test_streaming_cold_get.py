#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import json
import unittest
import logging

from typing import Optional
import pytest
from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout

from tests.integration import REMOTE_SET, REMOTE_BUCKET, CLUSTER_ENDPOINT
from tests.integration.sdk import TEST_RETRY_CONFIG, DEFAULT_TEST_CLIENT
from tests.utils import random_string
from tests.const import MIB, GIB, TEST_TIMEOUT

from aistore.sdk import Bucket, Object, Client
from aistore.sdk.const import HTTP_METHOD_PATCH


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

STREAMING_COLD_GET = 1 << 12  # 4096


# pylint: disable=too-few-public-methods
class MidStreamDropper:
    """
    Simulates a mid-stream disconnect by raising `ConnectionResetError` after a
    specified number of bytes have been read.
    """

    def __init__(self, stream, fail_after_bytes):
        self.stream = stream
        self.read_bytes = 0
        self.fail_after = fail_after_bytes

    def read(self, size=-1):
        # Simulate fail-before-read if limit already crossed
        if self.read_bytes >= self.fail_after:
            raise ConnectionResetError("Simulated mid-read disconnect")

        chunk = self.stream.read(size)

        self.read_bytes += len(chunk)

        # Simulate fail-after-read if this chunk crosses the limit
        if self.read_bytes >= self.fail_after:
            raise ConnectionResetError("Simulated mid-read disconnect")

        return chunk


class TestStreamingColdGet(unittest.TestCase):
    bucket: Optional[Bucket]
    object: Optional[Object]
    OBJECT_NAME = f"TestStreamingColdGet-{random_string(6)}"
    OBJECT_SIZE = GIB  # 1 GiB object for testing

    @classmethod
    def setUpClass(cls) -> None:
        if not REMOTE_SET:
            return

        cls.client = DEFAULT_TEST_CLIENT

        provider, bucket_name = REMOTE_BUCKET.split("://")
        cls.bucket = cls.client.bucket(bucket_name, provider=provider)
        cls.object = cls.bucket.object(cls.OBJECT_NAME)

        # Change client's timeout to None for the initial upload to avoid timeout
        # issues on large objects
        Client(CLUSTER_ENDPOINT, timeout=None).bucket(
            bucket_name, provider=provider
        ).object(cls.OBJECT_NAME).get_writer().put_content(
            content=b"0" * cls.OBJECT_SIZE
        )

    @classmethod
    def tearDownClass(cls) -> None:
        if not REMOTE_SET:
            return
        cls.object.delete()

    def setUp(self) -> None:
        # Evict the object before each test
        eviction_job = self.bucket.objects(obj_names=[self.OBJECT_NAME]).evict()
        result = self.client.job(job_id=eviction_job).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)

        self.bucket_uri = f"{self.bucket.provider.value}://{self.bucket.name}"

        # Enable Streaming-Cold-GET for the bucket before each test
        self.toggle_streaming_cold_get(enable=True)
        features = self.get_bucket_features()
        self.assertTrue(
            features & STREAMING_COLD_GET,
            "Streaming-Cold-GET feature is not enabled on the bucket.",
        )

    def toggle_streaming_cold_get(self, enable: bool = True) -> None:
        feature_value = str(STREAMING_COLD_GET) if enable else "0"

        self.bucket.make_request(
            method=HTTP_METHOD_PATCH,
            action="set-bprops",
            value={"features": feature_value},
        )

    def get_bucket_features(self) -> int:
        """
        Get the features value from the bucket info.
        """
        bucket_info_str = self.bucket.info()[0]
        try:
            bucket_props = json.loads(bucket_info_str)
            return int(bucket_props.get("features", 0))
        except json.JSONDecodeError as exc:
            raise ValueError("Failed to parse bucket info JSON") from exc

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    @pytest.mark.extended
    def test_streaming_cold_get_enabled_read_all(self):
        """
        Test that the object content size matches the expected size
        when reading with Streaming-Cold-GET enabled.

        Even if we are trying to read the entire object, the streaming
        cold get feature sends 200 response as soon as it starts reading.
        """
        content = self.object.get_reader().read_all()
        self.assertEqual(
            len(content),
            self.OBJECT_SIZE,
            "Object content size mismatch when reading with Streaming-Cold-GET enabled.",
        )

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    @pytest.mark.extended
    def test_streaming_cold_get_enabled_read_raw(self):
        """
        Test that the object content size matches the expected size
        when reading with Streaming-Cold-GET enabled.
        """
        chunk = (
            self.object.get_reader()
            .raw()
            .raw.read(MIB)  # Read the first 1 MB chunk directly using raw
        )
        self.assertEqual(len(chunk), MIB, "No initial chunk received.")

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    @pytest.mark.extended
    def test_streaming_cold_get_enabled_obj_file(self):
        """
        Test if we receive the first chunk before the timeout.
        """
        reader = self.object.get_reader().as_file()
        first_chunk = reader.read(MIB)  # Explicitly read first chunk (1 MB)

        self.assertEqual(len(first_chunk), MIB, "No initial chunk received.")

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    @pytest.mark.extended
    def test_streaming_cold_get_disabled(self):
        """
        Test that a `ConnectionError` (here `RequestsConnectionError`) is raised when attempting to read
        a huge object with Streaming-Cold-GET disabled.
        """
        # Disable Streaming-Cold-GET on the bucket
        self.toggle_streaming_cold_get(enable=False)

        features = self.get_bucket_features()
        self.assertEqual(
            features, 0, "Streaming-Cold-GET was not disabled on the bucket."
        )

        # Read the object with smaller timeout to trigger the connection error
        obj = (
            Client(
                CLUSTER_ENDPOINT,
                retry_config=TEST_RETRY_CONFIG,
                timeout=(
                    3,
                    5,
                ),  # Use a shorter timeout so that whole object cannot be read
            )
            .bucket(
                self.bucket.name,
                provider=self.bucket.provider.value,
            )
            .object(self.OBJECT_NAME)
        )

        with self.assertRaises((RequestsConnectionError, Timeout)):
            obj.get_reader().as_file().read()

        # Verify that the object is not cached
        assert (
            not obj.props.present
        ), "The object should not be cached when reading partially with Streaming-Cold-GET enabled."

        with self.assertRaises((RequestsConnectionError, Timeout)):
            obj.get_reader().read_all()

        # Verify that the object is not cached
        assert (
            not obj.props.present
        ), "The object should not be cached when reading partially with Streaming-Cold-GET enabled."

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    @pytest.mark.extended
    def test_streaming_cold_get_cached(self):
        """
        Test that the object will be cached only when its read in entirety
        with Streaming-Cold-GET enabled.
        """
        # Read only partial object
        content = self.object.get_reader().as_file().read(MIB)
        self.assertEqual(
            len(content),
            MIB,
            "The content should be only be 1 MiB when reading "
            "partially from the object with Streaming-Cold-GET enabled.",
        )
        # Verify that the object is not cached
        assert (
            not self.object.props.present
        ), "The object should not be cached when reading partially with Streaming-Cold-GET enabled."

        # Read the entire object to cache it
        full_content = self.object.get_reader().as_file().read()
        self.assertEqual(
            len(full_content),
            self.OBJECT_SIZE,
            "The full content should be read when reading the entire object with Streaming-Cold-GET enabled.",
        )
        # Verify that the object is now cached
        assert (
            self.object.props.present
        ), "The object should be cached after reading the entire object with Streaming-Cold-GET enabled."

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    @pytest.mark.extended
    def test_conn_err_mid_stream(self):
        """
        Test that `ConnectionResetError` is raised when a simulated mid-stream disconnect occurs.
        """
        stream = self.object.get_reader().raw()
        mid_stream_reader = MidStreamDropper(stream, fail_after_bytes=100 * MIB)

        with self.assertRaises(ConnectionResetError):
            while True:
                data = mid_stream_reader.read(128 * 1024)  # 128 KiB chunks
                if not data:
                    break
        # Verify that the object is not cached after a mid-stream disconnect
        assert (
            not self.object.props.present
        ), "The object should not be cached there was a problem in reading Streaming-Cold-GET enabled."
