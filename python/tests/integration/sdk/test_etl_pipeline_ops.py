#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
import unittest
import pytest

from aistore.sdk.errors import AISError
from aistore.sdk.etl import ETLConfig
from aistore.sdk.etl.etl_const import ETL_COMM_HPUSH, ETL_COMM_HPULL, ETL_COMM_WS
from aistore.sdk.etl.webserver.fastapi_server import FastAPIServer

from tests.integration.sdk import DEFAULT_TEST_CLIENT
from tests.utils import random_string, create_and_put_object, cases
from tests.const import TEST_TIMEOUT


# pylint: disable=unused-variable
class TestETLPipelineOps(unittest.TestCase):
    def setUp(self) -> None:
        self.client = DEFAULT_TEST_CLIENT
        self.bucket = self.client.bucket(random_string()).create()

        # Create multiple test objects using create_and_put_object
        self.test_objects = {}

        # Create objects with different sizes and names
        test_configs = [
            ("pipeline.txt", 128),
            ("test-file.dat", 256),
            ("sample.bin", 512),
            ("small.txt", 64),
            ("medium.dat", 1024),
            ("large.bin", 2048),
            ("empty.txt", 0),
        ]

        for name, size in test_configs:
            _, content = create_and_put_object(
                client=self.client,
                bck=self.bucket.as_model(),
                obj_name=name,
                obj_size=size,
            )
            self.test_objects[name] = content

        # Track created ETLs for cleanup
        self._etl_names = []

    def tearDown(self) -> None:
        self.bucket.delete(missing_ok=True)
        for etl_name in self._etl_names:
            try:
                self.client.etl(etl_name).stop()
                self.client.etl(etl_name).delete()
            except AISError:
                pass

    def _register_etl(self, stage_name: str, comm_type: str = ETL_COMM_HPUSH):
        """Register an ETL based on stage name and communication type."""
        etl_name = f"etl-{stage_name}-" + random_string(8)
        etl = self.client.etl(etl_name)

        if stage_name == "upper":

            @etl.init_class(comm_type=comm_type)
            class UpperCaseETL(FastAPIServer):
                def transform(self, data: bytes, *_args) -> bytes:
                    return data.upper()

        elif stage_name == "reverse":

            @etl.init_class(comm_type=comm_type)
            class ReverseETL(FastAPIServer):
                def transform(self, data: bytes, *_args) -> bytes:
                    return data[::-1]

        elif stage_name == "append":

            @etl.init_class(comm_type=comm_type)
            class AppendETL(FastAPIServer):
                def transform(self, data: bytes, *_args) -> bytes:
                    return data + b"::DONE"

        else:
            raise ValueError(f"Unknown stage: {stage_name}")

        self._etl_names.append(etl.name)
        return etl

    # pylint: disable=too-many-locals
    @pytest.mark.etl
    @cases(
        (
            ["upper", "reverse", "append"],
            [ETL_COMM_WS, ETL_COMM_HPULL, ETL_COMM_WS],
            lambda x: x.upper()[::-1] + b"::DONE",
        ),
        (
            ["upper", "upper", "reverse"],
            [ETL_COMM_HPUSH, ETL_COMM_WS, ETL_COMM_HPUSH],
            lambda x: x.upper()[::-1],
        ),
        (
            ["append", "reverse"],
            [ETL_COMM_HPUSH, ETL_COMM_HPULL],
            lambda x: (x + b"::DONE")[::-1],
        ),
    )
    def test_etl_pipeline_configurations(self, test_case):
        pipeline_stages, comm_types, transform_func = test_case

        with self.subTest(stages=pipeline_stages, comm_types=comm_types):
            self.setUp()
            try:
                self.assertEqual(
                    len(pipeline_stages),
                    len(comm_types),
                    "Pipeline stages and communication types must have the same length",
                )

                # Register ETLs for each stage with specified communication types
                etls = [
                    self._register_etl(stage, comm_type)
                    for stage, comm_type in zip(pipeline_stages, comm_types)
                ]

                # Build pipeline by chaining ETLs
                pipeline = etls[0]
                for etl in etls[1:]:
                    pipeline = pipeline >> etl

                # Test inline transformation on single object
                first_obj_name = list(self.test_objects.keys())[0]
                first_content = self.test_objects[first_obj_name]
                transformed = (
                    self.bucket.object(first_obj_name)
                    .get_reader(etl=ETLConfig(name=pipeline))
                    .read_all()
                )
                self.assertEqual(
                    transformed,
                    transform_func(first_content),
                    f"Inline transform failed for stages {pipeline_stages}",
                )

                # Test bucket-to-bucket offline transformation
                dst_bucket = self.client.bucket(random_string()).create()
                job_id = self.bucket.transform(
                    etl_name=pipeline.name,
                    etl_pipeline=pipeline.pipeline,
                    to_bck=dst_bucket,
                )
                result = self.client.job(job_id).wait_for_idle(timeout=TEST_TIMEOUT)
                self.assertTrue(result.success)

                # Verify all transformed objects in destination bucket
                for obj_name, original_content in self.test_objects.items():
                    self.assertEqual(
                        dst_bucket.object(obj_name).get_reader().read_all(),
                        transform_func(original_content),
                        f"Bucket transform failed for stages {pipeline_stages} on object {obj_name}",
                    )

                dst_bucket.delete(missing_ok=True)

            finally:
                self.tearDown()

    @pytest.mark.etl
    def test_pipeline_with_missing_middle_stage(self):
        etl_upper = self._register_etl("upper", ETL_COMM_HPUSH)

        missing_middle_name = "etl-missing-" + random_string(8)
        etl_missing = self.client.etl(missing_middle_name)
        # Note: not calling init_class() on purpose

        etl_reverse = self._register_etl("reverse", ETL_COMM_WS)

        composed = etl_upper >> etl_missing >> etl_reverse

        self.assertEqual(composed.pipeline, [etl_missing.name, etl_reverse.name])

        # Test that inline GET through the broken pipeline fails
        with self.assertRaises(AISError):
            self.bucket.object(list(self.test_objects.keys())[0]).get_reader(
                etl=ETLConfig(name=composed)
            ).read_all()

        # Test that bucket-to-bucket transform with broken pipeline also fails
        dst_bucket = self.client.bucket(random_string()).create()
        with self.assertRaises(AISError):
            self.bucket.transform(
                etl_name=composed.name,
                etl_pipeline=composed.pipeline,
                to_bck=dst_bucket,
            )
        dst_bucket.delete(missing_ok=True)

    @pytest.mark.etl
    def test_copy_object_with_etl_pipeline(self):
        """Test copying an object with ETL pipeline transformation."""
        dest_bucket = self.client.bucket(
            "copy-etl-pipeline-dest" + random_string()
        ).create()

        try:
            source_obj_name = "pipeline.txt"
            source_content = self.test_objects[source_obj_name]
            source_obj = self.bucket.object(source_obj_name)

            # Create ETL pipeline: upper -> reverse (two-stage pipeline)
            etl_upper = self._register_etl("upper", ETL_COMM_HPUSH)
            etl_reverse = self._register_etl("reverse", ETL_COMM_WS)

            # Copy with ETL pipeline transformation
            dest_obj = dest_bucket.object(source_obj_name)
            etl_config = ETLConfig(etl_upper >> etl_reverse)
            response = source_obj.copy(dest_obj, etl=etl_config)
            self.assertIn(response.status_code, [200, 204])

            # Verify the copied object has the transformed content
            copied_content = dest_obj.get_reader().read_all()
            self.assertEqual(
                copied_content,
                source_content.upper()[::-1],
                "Copied object should have pipeline-transformed content",
            )

            # Verify original object is unchanged
            original_content = source_obj.get_reader().read_all()
            self.assertEqual(
                original_content,
                source_content,
                "Original object should remain unchanged after copy with ETL",
            )

        finally:
            dest_bucket.delete(missing_ok=True)
