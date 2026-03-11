#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

"""
Integration tests for streaming ETL transforms (transform_stream).

Tests deploy actual ETL pods on a K8s cluster and verify streaming works
end-to-end across inline GET, offline bucket-to-bucket, with various
communication types, direct-put settings, and FQN modes.

Objects are 1 MB each and the streaming server reads in 4 KB chunks,
guaranteeing ~256 iterations per object to verify actual streaming behavior.
"""

import unittest

import pytest

from aistore.sdk.etl import ETLConfig
from aistore.sdk.etl.etl_const import ETL_COMM_HPUSH, ETL_COMM_HPULL
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer
from aistore.sdk.errors import AISError

from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import random_string
from tests.const import TEST_TIMEOUT_LONG

OBJ_SIZE = 1 * 1024 * 1024  # 1 MB per object
NUM_OBJECTS = 5
STREAM_CHUNK = 4096  # 4 KB — forces ~256 iterations per 1 MB object


def _generate_content(idx: int) -> bytes:
    """Generate 1 MB of repeating lowercase ASCII."""
    base = f"streaming test object number {idx} abcdefghijklmnopqrstuvwxyz ".encode()
    return (base * (OBJ_SIZE // len(base) + 1))[:OBJ_SIZE]


# pylint: disable=unused-variable


@pytest.mark.etl
class TestETLStreaming(unittest.TestCase):
    """Integration tests for transform_stream across all ETL workflows."""

    def setUp(self):
        self.client = DEFAULT_TEST_CLIENT
        self.bucket = self.client.bucket(
            bck_name="stream-test-" + random_string(5)
        ).create()
        self.etl_name = "etl-stream-" + random_string(5)

        # Upload 5 x 1 MB objects
        self.objects = {}
        for i in range(NUM_OBJECTS):
            name = f"obj_{i}.txt"
            content = _generate_content(i)
            self.bucket.object(name).get_writer().put_content(content)
            self.objects[name] = content

    def tearDown(self):
        self.bucket.delete(missing_ok=True)
        try:
            self.client.etl(self.etl_name).stop()
            self.client.etl(self.etl_name).delete()
        except (AISError, Exception):
            pass

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _init_streaming_etl(
        self,
        comm_type=ETL_COMM_HPUSH,
        direct_put=True,
        direct_file_access=False,
        obj_timeout="45s",
    ):
        etl = self.client.etl(self.etl_name)

        @etl.init_class(
            comm_type=comm_type,
            direct_put=direct_put,
            direct_file_access=direct_file_access,
            obj_timeout=obj_timeout,
        )
        class StreamUpper(FastAPIServer):
            """Upper-case input in 4 KB chunks via streaming."""

            def transform_stream(self, reader, path, etl_args):
                while True:
                    chunk = reader.read(STREAM_CHUNK)
                    if not chunk:
                        break
                    yield chunk.upper()

        return etl

    def _init_buffered_etl(self, etl_name):
        etl = self.client.etl(etl_name)

        @etl.init_class(
            comm_type=ETL_COMM_HPUSH,
            direct_put=True,
            obj_timeout="45s",
        )
        class BufferedUpper(FastAPIServer):
            """Upper-case input via buffered transform."""

            def transform(self, data, path, etl_args):
                if isinstance(data, str):
                    with open(data, "rb") as f:
                        data = f.read()
                return data.upper()

        return etl

    def _verify_inline(self, etl_name=None):
        """GET each object through ETL and verify upper-cased output."""
        name = etl_name or self.etl_name
        for obj_name, content in self.objects.items():
            result = (
                self.bucket.object(obj_name)
                .get_reader(etl=ETLConfig(name=name))
                .read_all()
            )
            self.assertEqual(
                len(result),
                len(content),
                f"{obj_name}: size mismatch ({len(result)} != {len(content)})",
            )
            self.assertEqual(
                result,
                content.upper(),
                f"{obj_name}: content mismatch",
            )

    def _verify_offline(self, dst_bucket):
        """Check all objects in destination bucket match upper-cased source."""
        for obj_name, content in self.objects.items():
            result = dst_bucket.object(obj_name).get_reader().read_all()
            self.assertEqual(
                len(result),
                len(content),
                f"{obj_name}: size mismatch ({len(result)} != {len(content)})",
            )
            self.assertEqual(
                result,
                content.upper(),
                f"{obj_name}: content mismatch",
            )

    def _run_offline(self, etl_name=None):
        """Run bucket-to-bucket transform and verify."""
        name = etl_name or self.etl_name
        dst = self.client.bucket("stream-dst-" + random_string(5)).create()
        try:
            job_id = self.bucket.transform(
                etl_name=name,
                to_bck=dst,
                cont_on_err=True,
            )
            self.client.job(job_id).wait(timeout=TEST_TIMEOUT_LONG)

            dst_objs = list(dst.list_all_objects())
            self.assertEqual(
                len(dst_objs),
                NUM_OBJECTS,
                f"expected {NUM_OBJECTS} objects in dst, got {len(dst_objs)}",
            )
            self._verify_offline(dst)
        finally:
            dst.delete(missing_ok=True)

    # ------------------------------------------------------------------
    # Inline GET tests
    # ------------------------------------------------------------------

    @pytest.mark.etl
    def test_streaming_inline_hpush(self):
        """Inline GET with hpush communication — streaming transform."""
        self._init_streaming_etl(comm_type=ETL_COMM_HPUSH)
        self._verify_inline()

    @pytest.mark.etl
    def test_streaming_inline_hpull(self):
        """Inline GET with hpull communication — streaming transform."""
        self._init_streaming_etl(comm_type=ETL_COMM_HPULL)
        self._verify_inline()

    @pytest.mark.etl
    def test_streaming_inline_fqn(self):
        """Inline GET with FQN (direct file access) — streaming transform."""
        self._init_streaming_etl(comm_type=ETL_COMM_HPUSH, direct_file_access=True)
        self._verify_inline()

    # ------------------------------------------------------------------
    # Offline bucket-to-bucket tests
    # ------------------------------------------------------------------

    @pytest.mark.etl
    def test_streaming_offline_direct_put(self):
        """Offline transform with direct-put enabled — streaming."""
        self._init_streaming_etl(direct_put=True)
        self._run_offline()

    @pytest.mark.etl
    def test_streaming_offline_no_direct_put(self):
        """Offline transform without direct-put — streaming."""
        self._init_streaming_etl(direct_put=False)
        self._run_offline()

    @pytest.mark.etl
    def test_streaming_offline_fqn(self):
        """Offline transform with FQN + direct-put — streaming."""
        self._init_streaming_etl(direct_put=True, direct_file_access=True)
        self._run_offline()

    @pytest.mark.etl
    def test_streaming_offline_hpull(self):
        """Offline transform with hpull + direct-put — streaming."""
        self._init_streaming_etl(comm_type=ETL_COMM_HPULL, direct_put=True)
        self._run_offline()

    # ------------------------------------------------------------------
    # Streaming vs buffered comparison
    # ------------------------------------------------------------------

    @pytest.mark.etl
    def test_streaming_vs_buffered_identical(self):
        """Verify streaming and buffered produce byte-identical output."""
        # Init streaming ETL
        self._init_streaming_etl(direct_put=True)

        # Init buffered ETL with a different name
        buffered_name = "etl-buffered-" + random_string(5)
        self._init_buffered_etl(buffered_name)

        dst_stream = self.client.bucket("cmp-stream-" + random_string(5)).create()
        dst_buffered = self.client.bucket("cmp-buffer-" + random_string(5)).create()

        try:
            # Run both transforms
            job_stream = self.bucket.transform(
                etl_name=self.etl_name, to_bck=dst_stream, cont_on_err=True
            )
            job_buffered = self.bucket.transform(
                etl_name=buffered_name, to_bck=dst_buffered, cont_on_err=True
            )
            self.client.job(job_stream).wait(timeout=TEST_TIMEOUT_LONG)
            self.client.job(job_buffered).wait(timeout=TEST_TIMEOUT_LONG)

            # Compare outputs
            for obj_name in self.objects:
                stream_data = dst_stream.object(obj_name).get_reader().read_all()
                buffered_data = dst_buffered.object(obj_name).get_reader().read_all()
                self.assertEqual(
                    len(stream_data),
                    len(buffered_data),
                    f"{obj_name}: size mismatch between streaming and buffered",
                )
                self.assertEqual(
                    stream_data,
                    buffered_data,
                    f"{obj_name}: content mismatch between streaming and buffered",
                )
        finally:
            dst_stream.delete(missing_ok=True)
            dst_buffered.delete(missing_ok=True)
            try:
                self.client.etl(buffered_name).stop()
                self.client.etl(buffered_name).delete()
            except (AISError, Exception):
                pass
