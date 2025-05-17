#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
#

import io
import json
import tarfile
import random
from pathlib import Path
from typing import Literal

import pytest
import yaml

from aistore.sdk import Bucket
from aistore.sdk.dsort import (
    DsortFramework,
    DsortShardsGroup,
    DsortAlgorithm,
    ExternalKeyMap,
)
from aistore.sdk.multiobj import ObjectRange, ObjectNames
from tests.const import TEST_TIMEOUT, MB, KB
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import cases, random_string, create_random_tarballs

TAR_NUM_FILES = 100
MIN_SHARD_SIZE = 50 * KB


class TestDsortOps(ParallelTestBase):
    def _upload_dir(self, dir_name, bck):
        bck.put_files(dir_name)

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _generate_tar(
        self,
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
                txt_file = self.local_test_files.joinpath(
                    f"shard-{prefix}-file-{i}.txt"
                )
                with open(txt_file, "w", encoding="utf-8") as text:
                    text.write(random_string())
                # Add the file to the tarfile
                tar.add(txt_file)
                # Remove the file after adding it to the tarfile
                Path(txt_file).unlink()

                if key_extension:
                    key_file_name = self.local_test_files.joinpath(
                        f"shard-{prefix}-file-{i}{key_extension}"
                    )
                    with open(key_file_name, "w", encoding="utf-8") as key_file:
                        if key_type == "int":
                            key_file.write(str(random.randint(0, 1000)))
                        elif key_type == "float":
                            key_file.write(str(random.uniform(0, 1000)))
                        elif key_type == "string":
                            key_file.write(random_string())
                    tar.add(key_file_name)
                    Path(key_file_name).unlink()

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _generate_shards(
        self,
        bck: Bucket,
        tar_enum,
        num_shards,
        num_files,
        key_extension=None,
        key_type: Literal["int", "float", "string"] = None,
    ):
        shard_names = []
        out_dir = self.local_test_files.joinpath(bck.name)
        out_dir.mkdir(exist_ok=True)
        for shard_index in range(num_shards):
            name = f"{bck.name}-{shard_index}.tar"
            filename = out_dir.joinpath(name)
            self._generate_tar(
                filename, shard_index, tar_enum, num_files, key_extension, key_type
            )
            shard_names.append(name)
        bck.put_files(str(out_dir))
        return shard_names

    @staticmethod
    def _get_object_content_map(bck, object_names):
        expected_contents = {}
        for obj in object_names:
            output_bytes = bck.object(obj).get_reader().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                for tar in result_tar:
                    expected_contents[tar.name] = result_tar.extractfile(
                        tar.name
                    ).read()
        return expected_contents

    # pylint: disable=too-many-locals
    @pytest.mark.nonparallel("potentially causes resilver")
    @cases(("gnu", tarfile.GNU_FORMAT, 2, 3), ("pax", tarfile.PAX_FORMAT, 2, 3))
    def test_dsort_json(self, test_case):
        self._test_dsort_from_spec(test_case, spec_type="json")

    @pytest.mark.nonparallel("potentially causes resilver")
    @cases(("gnu", tarfile.GNU_FORMAT, 2, 3), ("pax", tarfile.PAX_FORMAT, 2, 3))
    def test_dsort_yaml(self, test_case):
        self._test_dsort_from_spec(test_case, spec_type="yaml")

    def _test_dsort_from_spec(self, test_case, spec_type):
        tar_type, tar_format, num_shards, num_files = test_case
        in_bck = self._create_bucket(tar_type + "-in")
        out_bck = self._create_bucket(tar_type + "-out")
        # create tars as objects in buckets
        shards = self._generate_shards(in_bck, tar_format, num_shards, num_files)
        # Read created objects to get expected output after dsort
        expected_contents = self._get_object_content_map(
            bck=in_bck, object_names=shards
        )

        spec = {
            "input_extension": ".tar",
            "input_bck": {"name": in_bck.name, "provider": in_bck.provider.value},
            "output_bck": {"name": out_bck.name, "provider": out_bck.provider.value},
            "input_format": {"template": in_bck.name + "-{0..9}"},
            "output_format": "out-shard-{0..9}",
            "output_extension": ".tar",
            "output_shard_size": "10KB",
            "algorithm": {},
            "description": "Dsort Integration Test",
        }
        assert spec_type in ["json", "yaml"]
        if spec_type == "json":
            spec_file = self.local_test_files.joinpath("spec.json")
            with open(spec_file, "w", encoding="utf-8") as outfile:
                outfile.write(json.dumps(spec, indent=4))
        else:
            spec_file = self.local_test_files.joinpath("spec.yaml")
            with open(spec_file, "w", encoding="utf-8") as outfile:
                yaml.dump(spec, outfile, default_flow_style=False)

        dsort = self.client.dsort()
        dsort.start(spec_file)

        dsort.wait(timeout=TEST_TIMEOUT)
        output_bytes = out_bck.object("out-shard-0.tar").get_reader().read_all()
        output = io.BytesIO(output_bytes)
        result_contents = {}
        with tarfile.open(fileobj=output) as result_tar:
            for tar in result_tar:
                result_contents[tar.name] = result_tar.extractfile(tar.name).read()

        self.assertEqual(expected_contents, result_contents)

    @pytest.mark.nonparallel("potentially causes resilver")
    def test_dsort_with_ekm(self):
        input_bck = self._create_bucket("ekm-in")
        out_bck = self._create_bucket("ekm-out")
        in_dir = self.local_test_files.joinpath(input_bck.name)
        in_dir.mkdir(exist_ok=True)
        filename_list, extension_list, num_input_shards = create_random_tarballs(
            TAR_NUM_FILES, 3, MB, in_dir
        )
        input_bck.put_files(in_dir)

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

        for output_shard in out_bck.list_all_objects_iter():
            output_bytes = output_shard.get_reader().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                for tar in result_tar:
                    tar_filepath = Path(tar.name)
                    # Prefix of shard name (excluding number and ext) should match the stem of each archived file name
                    self.assertTrue(output_shard.name.startswith(tar_filepath.stem))
                    self.assertIn(tar_filepath.stem, filename_list)
                    self.assertIn(tar_filepath.suffix[1:], extension_list)

    @pytest.mark.nonparallel("potentially causes resilver")
    def test_algorithm_alphanumeric(self):
        input_bck = self._create_bucket("alpha-in")
        out_bck = self._create_bucket("alpha-out")

        num_shards, num_files = 10, 100
        self._generate_shards(input_bck, tarfile.GNU_FORMAT, num_shards, num_files)

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=input_bck.as_model(),
                role="input",
                format=ObjectRange.from_string(input_bck.name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=out_bck.as_model(),
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
        for output_shard in out_bck.list_all_objects_iter(prefix="output-shards-"):
            output_bytes = output_shard.get_reader().read_all()
            output = io.BytesIO(output_bytes)
            with tarfile.open(fileobj=output) as result_tar:
                tar_names.extend([tar.name for tar in result_tar])

        self.assertEqual(tar_names, sorted(tar_names))
        self.assertEqual(len(tar_names), num_shards * num_files)

    def _create_shuffle_dsort_framework(self, input_bck, out_bck) -> DsortFramework:
        return DsortFramework(
            input_shards=DsortShardsGroup(
                bck=input_bck.as_model(),
                role="input",
                format=ObjectRange.from_string(input_bck.name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=out_bck.as_model(),
                role="output",
                format=ObjectRange.from_string("output-shards-{000..100}"),
                extension=".tar",
            ),
            algorithm=DsortAlgorithm(kind="shuffle"),
            description="test_algorithm_shuffle",
            output_shard_size="10KiB",
        )

    @pytest.mark.nonparallel("potentially causes resilver")
    def test_algorithm_shuffle(self):
        input_bck = self._create_bucket("shuffle-in")
        out_bck = self._create_bucket("shuffle-out")

        num_shards, num_files = 10, 100
        self._generate_shards(input_bck, tarfile.GNU_FORMAT, num_shards, num_files)

        dsort_framework = self._create_shuffle_dsort_framework(input_bck, out_bck)

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.wait(timeout=TEST_TIMEOUT)
        tar_names = []
        for output_shard in self.client.bucket(out_bck.name).list_all_objects_iter(
            prefix="output-shards-"
        ):
            output_bytes = output_shard.get_reader().read_all()
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

    @pytest.mark.nonparallel("potentially causes resilver")
    @cases(
        (".loss", "int", False),
        (".cls", "float", False),
        (".smth", "string", False),
        (".loss", "int", True),
        (".cls", "float", True),
        (".smth", "string", True),
    )
    def test_algorithm_content(self, test_case):
        extension, content_key_type, missing_keys = test_case
        input_bck = self._create_bucket(f"{content_key_type}-in")
        out_bck = self.client.bucket(f"{content_key_type}-out")

        num_shards, num_files = 10, 20
        self._generate_shards(
            input_bck,
            tarfile.GNU_FORMAT,
            num_shards,
            num_files,
            extension,
            content_key_type,
        )

        dsort_framework = DsortFramework(
            input_shards=DsortShardsGroup(
                bck=input_bck.as_model(),
                role="input",
                format=ObjectRange.from_string(input_bck.name + "-{0..9}"),
                extension=".tar",
            ),
            output_shards=DsortShardsGroup(
                bck=out_bck.as_model(),
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
        for output_shard in out_bck.list_all_objects_iter(prefix="output-shards-"):
            output_bytes = output_shard.get_reader().read_all()
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

    @pytest.mark.nonparallel("potentially causes resilver")
    def test_abort(self):
        input_bck = self._create_bucket("abort-in")
        out_bck = self._create_bucket("abort-out")
        # Create enough files to make the dSort job slow enough to abort
        self._generate_shards(input_bck, tarfile.GNU_FORMAT, 200, 1000)

        # reuse the shuffle framework from other tests -- it doesn't matter as we'll abort
        dsort_framework = self._create_shuffle_dsort_framework(input_bck, out_bck)

        dsort = self.client.dsort()
        dsort.start(dsort_framework)
        dsort.abort()
        dsort.wait(timeout=TEST_TIMEOUT)
        for job_info in dsort.get_job_info().values():
            self.assertTrue(job_info.metrics.aborted)
            self.assertEqual(1, len(job_info.metrics.errors))
