import io
import json
import shutil
import tarfile
import unittest
from pathlib import Path

from aistore import Client
from tests.integration import CLUSTER_ENDPOINT, TEST_TIMEOUT
from tests.unit.sdk.test_utils import test_cases
from tests.utils import random_string


class TestDsortOps(unittest.TestCase):
    def setUp(self) -> None:
        self.client = Client(CLUSTER_ENDPOINT)
        self.temp_dir = Path("tmp")
        try:
            self.temp_dir.mkdir()
        except FileExistsError:
            shutil.rmtree(self.temp_dir)
            self.temp_dir.mkdir()
        self.buckets = []

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir)
        for bucket in self.buckets:
            self.client.bucket(bucket).delete(missing_ok=True)

    def _upload_dir(self, dir_name, bck_name):
        bck = self.client.bucket(bck_name).create(exist_ok=True)
        self.buckets.append(bck_name)
        bck.put_files(dir_name)

    @staticmethod
    def _generate_tar(filename, prefix, tar_format, num_files):
        with tarfile.open(filename, "w|", format=tar_format) as tar:
            for i in range(num_files):
                # Create a file name and write random text to it
                filename = f"shard-{prefix}-file-{i}.txt"
                with open(filename, "w", encoding="utf-8") as text:
                    text.write(random_string())
                # Add the file to the tarfile
                tar.add(filename)
                # Remove the file after adding it to the tarfile
                Path(filename).unlink()

    def _generate_shards(self, tar_type, tar_enum, num_shards, num_files):
        shard_names = []
        out_dir = Path(self.temp_dir).joinpath(tar_type)
        out_dir.mkdir(exist_ok=True)
        for shard_index in range(num_shards):
            name = f"{tar_type}-{shard_index}.tar"
            filename = out_dir.joinpath(name)
            self._generate_tar(filename, shard_index, tar_enum, num_files)
            shard_names.append(name)
        self._upload_dir(out_dir, tar_type)
        return shard_names

    def _get_object_content_map(self, bucket_name, object_names):
        expected_contents = {}
        for obj in object_names:
            output_bytes = self.client.bucket(bucket_name).object(obj).get().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                for tar in result_tar:
                    expected_contents[tar.name] = result_tar.extractfile(
                        tar.name
                    ).read()
        return expected_contents

    def _start_with_spec(self, input_bck_name, out_bck_name, input_object_prefix):
        spec = {
            "extension": ".tar",
            "bck": {"name": input_bck_name},
            "output_bck": {"name": out_bck_name},
            "input_format": {"template": input_object_prefix + "-{0..1}"},
            "output_format": "out-shard-{0..9}",
            "output_shard_size": "20MB",
            "description": "Dsort Integration Test",
        }
        spec_file = self.temp_dir.joinpath("spec.json")
        with open(spec_file, "w", encoding="utf-8") as outfile:
            outfile.write(json.dumps(spec, indent=4))
        dsort = self.client.dsort()
        dsort.start(spec_file)
        return dsort

    @test_cases(("gnu", tarfile.GNU_FORMAT, 2, 3), ("pax", tarfile.PAX_FORMAT, 2, 3))
    def test_dsort(self, test_case):
        tar_type, tar_format, num_shards, num_files = test_case
        # create bucket for output
        out_bck_name = tar_type + "-out"
        self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)
        # create tars as objects in buckets
        shards = self._generate_shards(tar_type, tar_format, num_shards, num_files)
        # Read created objects to get expected output after dsort
        expected_contents = self._get_object_content_map(
            bucket_name=tar_type, object_names=shards
        )
        dsort = self._start_with_spec(
            input_bck_name=tar_type,
            out_bck_name=out_bck_name,
            input_object_prefix=tar_type,
        )
        dsort.wait(timeout=TEST_TIMEOUT)
        output_bytes = (
            self.client.bucket(out_bck_name).object("out-shard-0.tar").get().read_all()
        )
        output = io.BytesIO(output_bytes)
        result_contents = {}
        with tarfile.open(fileobj=output) as result_tar:
            for tar in result_tar:
                result_contents[tar.name] = result_tar.extractfile(tar.name).read()

        self.assertEqual(expected_contents, result_contents)

    def test_abort(self):
        input_bck_name = "abort"
        out_bck_name = "out"
        self.client.bucket(input_bck_name).create(exist_ok=True)
        self.buckets.append(input_bck_name)
        self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)
        # Create enough files to make the dSort job slow enough to abort
        self._generate_shards(input_bck_name, tarfile.GNU_FORMAT, 10, 1000)
        dsort = self._start_with_spec(
            input_bck_name=input_bck_name,
            out_bck_name=out_bck_name,
            input_object_prefix=input_bck_name,
        )
        dsort.abort()
        dsort.wait(timeout=TEST_TIMEOUT)
        metrics = dsort.metrics()
        for metric in metrics.values():
            self.assertTrue(metric.aborted)
            self.assertEqual(1, len(metric.errors))
