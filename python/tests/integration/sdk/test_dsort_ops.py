import io
import json
import shutil
import tarfile
import unittest
import random
from pathlib import Path
from typing import Literal
import yaml

from aistore import Client
from aistore.sdk.dsort import (
    DsortFramework,
    DsortShardsGroup,
    DsortAlgorithm,
    ExternalKeyMap,
)
from aistore.sdk.types import BucketModel
from aistore.sdk.multiobj import ObjectRange, ObjectNames
from tests.integration import CLUSTER_ENDPOINT
from tests.const import TEST_TIMEOUT
from tests.utils import test_cases, random_string, create_random_tarballs


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

    # pylint: disable=too-many-arguments
    @staticmethod
    def _generate_tar(
        filename,
        prefix,
        tar_format,
        num_files,
        key_extension=None,
        key_type: Literal["int", "float", "string"] = None,
    ):
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

                if key_extension:
                    key_file_name = f"shard-{prefix}-file-{i}{key_extension}"
                    with open(key_file_name, "w", encoding="utf-8") as key_file:
                        if key_type == "int":
                            key_file.write(str(random.randint(0, 1000)))
                        elif key_type == "float":
                            key_file.write(str(random.uniform(0, 1000)))
                        elif key_type == "string":
                            key_file.write(random_string())
                    tar.add(key_file_name)
                    Path(key_file_name).unlink()

    # pylint: disable=too-many-arguments
    def _generate_shards(
        self,
        bck_name,
        tar_enum,
        num_shards,
        num_files,
        key_extension=None,
        key_type: Literal["int", "float", "string"] = None,
    ):
        shard_names = []
        out_dir = Path(self.temp_dir).joinpath(bck_name)
        out_dir.mkdir(exist_ok=True)
        for shard_index in range(num_shards):
            name = f"{bck_name}-{shard_index}.tar"
            filename = out_dir.joinpath(name)
            self._generate_tar(
                filename, shard_index, tar_enum, num_files, key_extension, key_type
            )
            shard_names.append(name)
        self._upload_dir(out_dir, bck_name)
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

    # pylint: disable=too-many-locals
    @test_cases(("gnu", tarfile.GNU_FORMAT, 2, 3), ("pax", tarfile.PAX_FORMAT, 2, 3))
    def test_dsort_json(self, test_case):
        self._test_dsort_from_spec(test_case, spec_type="json")

    @test_cases(("gnu", tarfile.GNU_FORMAT, 2, 3), ("pax", tarfile.PAX_FORMAT, 2, 3))
    def test_dsort_yaml(self, test_case):
        self._test_dsort_from_spec(test_case, spec_type="yaml")

    def _test_dsort_from_spec(self, test_case, spec_type):
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

        spec = {
            "input_extension": ".tar",
            "input_bck": {"name": tar_type},
            "output_bck": {"name": out_bck_name},
            "input_format": {"template": tar_type + "-{0..9}"},
            "output_format": "out-shard-{0..9}",
            "output_extension": ".tar",
            "output_shard_size": "10KB",
            "algorithm": {},
            "description": "Dsort Integration Test",
        }
        if spec_type == "json":
            spec_file = Path(self.temp_dir.name).joinpath("spec.json")
            with open(spec_file, "w", encoding="utf-8") as outfile:
                outfile.write(json.dumps(spec, indent=4))
        elif spec_type == "yaml":
            spec_file = Path(self.temp_dir.name).joinpath("spec.yaml")
            with open(spec_file, "w", encoding="utf-8") as outfile:
                yaml.dump(spec, outfile, default_flow_style=False)

        dsort = self.client.dsort()
        dsort.start(spec_file)

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

    def test_dsort_with_ekm(self):
        input_bck_name, out_bck_name = "dsort-ekm-input", "dsort-ekm-output"
        input_bck = self.client.bucket(input_bck_name).create(exist_ok=True)
        self.buckets.append(input_bck_name)
        out_bck = self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)
        out_dir = Path(self.temp_dir).joinpath(input_bck_name)
        out_dir.mkdir(exist_ok=True)
        filename_list, extension_list, num_input_shards = create_random_tarballs(
            300, 3, 2**20, out_dir
        )
        self._upload_dir(out_dir, input_bck_name)

        ekm = ExternalKeyMap()
        for filename in filename_list:
            ekm[f"{filename}-%d.tar"] = ObjectNames(
                [f"{filename}.{ext}" for ext in extension_list]
            )

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=input_bck.as_model(),
                role="input",
                format=ObjectRange("input-shard-", 0, num_input_shards),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=out_bck.as_model(), role="output", format=ekm, extension=".tar"
            ),
            output_shard_size="100KiB",
            description="test_dsort_with_ekm",
        )

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.wait(timeout=TEST_TIMEOUT)

        for output_shard in self.client.bucket(out_bck_name).list_all_objects_iter():
            output_bytes = output_shard.get().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                for tar in result_tar:
                    tar_filepath = Path(tar.name)
                    # Prefix of shard name (excluding number and ext) should match the stem of each archived file name
                    self.assertTrue(output_shard.name.startswith(tar_filepath.stem))
                    self.assertIn(tar_filepath.stem, filename_list)
                    self.assertIn(tar_filepath.suffix[1:], extension_list)

    def test_algorithm_alphanumeric(self):
        input_bck_name, out_bck_name = "alphanumeric-input", "alphanumeric-out"
        self.client.bucket(input_bck_name).create(exist_ok=True)
        self.buckets.append(input_bck_name)
        self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)

        num_shards, num_files = 10, 1000
        self._generate_shards(input_bck_name, tarfile.GNU_FORMAT, num_shards, num_files)

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=BucketModel(name=input_bck_name),
                role="input",
                format=ObjectRange.from_string(input_bck_name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=BucketModel(name=out_bck_name),
                role="output",
                format=ObjectRange.from_string("output-shards-{000..100}"),
                extension=".tar",
            ),
            algorithm=DsortAlgorithm(kind="alphanumeric"),
            description="test_algorithm_alphanumeric",
            output_shard_size="10KiB",
        )

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.wait(timeout=TEST_TIMEOUT)
        tar_names = []
        for output_shard in self.client.bucket(out_bck_name).list_all_objects_iter(
            prefix="output-shards-"
        ):
            output_bytes = output_shard.get().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                tar_names.extend([tar.name for tar in result_tar])

        self.assertEqual(tar_names, sorted(tar_names))
        self.assertEqual(len(tar_names), num_shards * num_files)

    def test_algorithm_shuffle(self):
        input_bck_name, out_bck_name = "shuffle-input", "shuffle-out"
        self.client.bucket(input_bck_name).create(exist_ok=True)
        self.buckets.append(input_bck_name)
        self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)

        num_shards, num_files = 10, 1000
        self._generate_shards(input_bck_name, tarfile.GNU_FORMAT, num_shards, num_files)

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=BucketModel(name=input_bck_name),
                role="input",
                format=ObjectRange.from_string(input_bck_name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=BucketModel(name=out_bck_name),
                role="output",
                format=ObjectRange.from_string("output-shards-{000..100}"),
                extension=".tar",
            ),
            algorithm=DsortAlgorithm(kind="shuffle"),
            description="test_algorithm_shuffle",
            output_shard_size="10KiB",
        )

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.wait(timeout=TEST_TIMEOUT)
        tar_names = []
        for output_shard in self.client.bucket(out_bck_name).list_all_objects_iter(
            prefix="output-shards-"
        ):
            output_bytes = output_shard.get().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                tar_names.extend([tar.name for tar in result_tar])

        # Verify the tar names are in random order
        self.assertNotEqual(tar_names, sorted(tar_names))

        # Additional check: Shuffle the sorted list and ensure it doesn't match tar_names
        sorted_tar_names = sorted(tar_names)
        random.shuffle(sorted_tar_names)
        self.assertNotEqual(tar_names, sorted_tar_names)
        self.assertEqual(len(tar_names), num_shards * num_files)

    @test_cases(
        (".loss", "int", False),
        (".cls", "float", False),
        (".smth", "string", False),
        (".loss", "int", True),
        (".cls", "float", True),
        (".smth", "string", True),
    )
    def test_algorithm_content(self, test_case):
        extension, content_key_type, missing_keys = test_case
        input_bck_name = f"{content_key_type}-input"
        out_bck_name = f"{content_key_type}-out"
        self.client.bucket(input_bck_name).create(exist_ok=True)
        self.buckets.append(input_bck_name)
        self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)

        num_shards, num_files = 10, 20
        self._generate_shards(
            input_bck_name,
            tarfile.GNU_FORMAT,
            num_shards,
            num_files,
            extension,
            content_key_type,
        )

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=BucketModel(name=input_bck_name),
                role="input",
                format=ObjectRange.from_string(input_bck_name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=BucketModel(name=out_bck_name),
                role="output",
                format=ObjectRange.from_string("output-shards-{000..100}"),
                extension=".tar",
            ),
            algorithm=DsortAlgorithm(
                kind="content",
                extension=extension,
                content_key_type=content_key_type,
                missing_keys=missing_keys,
            ),
            description="test_algorithm_shuffle",
            output_shard_size="10KiB",
        )

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.wait(timeout=TEST_TIMEOUT)

        num_archived_files = 0
        last_file_name, last_value = "", None
        for output_shard in self.client.bucket(out_bck_name).list_all_objects_iter(
            prefix="output-shards-"
        ):
            output_bytes = output_shard.get().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as tar:
                for file_info in tar:
                    num_archived_files += 1
                    if file_info.name.endswith(extension):
                        # custom key files should go after the regular files
                        self.assertEqual(
                            file_info.name.rsplit(".", 1)[0],
                            last_file_name.rsplit(".", 1)[0],
                        )

                        # extract and convert key content
                        content = tar.extractfile(file_info).read().decode("utf-8")
                        if content_key_type == "int":
                            self.assertTrue(
                                last_value is None or last_value <= int(content)
                            )
                            last_value = int(content)
                        elif content_key_type == "float":
                            self.assertTrue(
                                last_value is None or last_value <= float(content)
                            )
                            last_value = float(content)
                        elif content_key_type == "string":
                            self.assertTrue(last_value is None or last_value <= content)
                            last_value = content
                    else:
                        last_file_name = file_info.name
        self.assertEqual(
            num_archived_files, 2 * num_shards * num_files
        )  # both key and content files

    def test_abort(self):
        input_bck_name = "abort"
        out_bck_name = "out"
        self.client.bucket(input_bck_name).create(exist_ok=True)
        self.buckets.append(input_bck_name)
        self.client.bucket(out_bck_name).create(exist_ok=True)
        self.buckets.append(out_bck_name)
        # Create enough files to make the dSort job slow enough to abort
        self._generate_shards(input_bck_name, tarfile.GNU_FORMAT, 10, 1000)

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=BucketModel(name=input_bck_name),
                role="input",
                format=ObjectRange.from_string(input_bck_name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=BucketModel(name=out_bck_name),
                role="output",
                format=ObjectRange.from_string("output-shards-{000..100}"),
                extension=".tar",
            ),
            algorithm=DsortAlgorithm(),
            description="test_algorithm_shuffle",
            output_shard_size="10KiB",
        )

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.abort()
        dsort.wait(timeout=TEST_TIMEOUT)
        for job_info in dsort.get_job_info().values():
            self.assertTrue(job_info.metrics.aborted)
            self.assertEqual(1, len(job_info.metrics.errors))
