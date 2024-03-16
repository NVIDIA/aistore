#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import random
import unittest
from datetime import datetime
from pathlib import Path

from aistore.sdk.const import AIS_VERSION, HEADER_CONTENT_LENGTH, UTF_ENCODING

from tests.const import (
    SMALL_FILE_SIZE,
    OBJ_NAME,
    OBJ_READ_TYPE_ALL,
    OBJ_READ_TYPE_CHUNK,
    TEST_TIMEOUT,
)
from tests.integration.sdk.remote_enabled_test import RemoteEnabledTest
from tests.utils import (
    random_string,
    cleanup_local,
    test_cases,
)
from tests.integration import CLUSTER_ENDPOINT, REMOTE_SET


# pylint: disable=unused-variable
class TestObjectOps(RemoteEnabledTest):
    def setUp(self) -> None:
        super().setUp()
        self.local_test_files = (
            Path().absolute().joinpath("object-ops-test-" + random_string(8))
        )

    def tearDown(self) -> None:
        super().tearDown()
        # Cleanup local files at end
        cleanup_local(str(self.local_test_files))

    def _test_get_obj(self, read_type, obj_name, exp_content):
        chunk_size = random.randrange(1, len(exp_content) + 10)
        stream = self._create_object(obj_name).get(chunk_size=chunk_size)

        self.assertEqual(stream.attributes.size, len(exp_content))
        self.assertNotEqual(stream.attributes.checksum_type, "")
        self.assertNotEqual(stream.attributes.checksum_value, "")
        self.assertNotEqual(stream.attributes.access_time, "")
        if not REMOTE_SET:
            self.assertNotEqual(stream.attributes.obj_version, "")
            self.assertEqual(stream.attributes.custom_metadata, {})
        if read_type == OBJ_READ_TYPE_ALL:
            obj = stream.read_all()
        else:
            obj = b""
            for chunk in stream:
                obj += chunk
        self.assertEqual(obj, exp_content)

    def _put_objects(self, num_obj, obj_size=None):
        name_to_content = {}
        for i in range(num_obj):
            obj_name = f"obj{ i }"
            content = self._create_object_with_content(
                obj_name=obj_name, obj_size=obj_size
            )
            name_to_content[obj_name] = content
        return name_to_content

    def test_put_content(self):
        content = b"content for the object"
        obj = self._create_object(OBJ_NAME)
        obj.put_content(content)
        res = obj.get()
        self.assertEqual(content, res.read_all())

    def test_put_file(self):
        self.local_test_files.mkdir()
        content = b"content for the object"
        filename = self.local_test_files.joinpath("test_file")
        with open(filename, "wb") as writer:
            writer.write(content)
        obj = self._create_object(OBJ_NAME)
        obj.put_file(filename)
        res = obj.get()
        self.assertEqual(content, res.read_all())

    def test_put_file_invalid(self):
        with self.assertRaises(ValueError):
            self.bucket.object("any").put_file("non-existent-file")
        self.local_test_files.mkdir()
        inner_dir = self.local_test_files.joinpath("inner_dir_not_file")
        inner_dir.mkdir()
        with self.assertRaises(ValueError):
            self.bucket.object("any").put_file(inner_dir)

    def test_put_head_get(self):
        objects = self._put_objects(5)
        for obj_name, content in objects.items():
            properties = self.bucket.object(obj_name).head()
            if not REMOTE_SET:
                self.assertEqual(properties[AIS_VERSION], "1")
            self.assertEqual(properties[HEADER_CONTENT_LENGTH], str(len(content)))
            for option in [OBJ_READ_TYPE_ALL, OBJ_READ_TYPE_CHUNK]:
                self._test_get_obj(option, obj_name, content)

    def test_get_with_writer(self):
        self.local_test_files.mkdir()
        filename = self.local_test_files.joinpath("test_get_with_writer.txt")
        objects = self._put_objects(10)
        all_content = b""
        for obj_name, content in objects.items():
            # Pass a writer that appends to a file
            with open(filename, "ab") as writer:
                self.bucket.object(obj_name).get(writer=writer)
            all_content += content
        # Verify file contents are written from each object
        with open(filename, "rb") as reader:
            output = reader.read()
            self.assertEqual(all_content, output)
        filename.unlink()

    def test_get_range(self):
        objects = self._put_objects(5)
        for obj_name, content in objects.items():
            resp = self.bucket.object(obj_name).get(byte_range="bytes=5-100").read_all()
            self.assertEqual(content[5:101], resp)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    @test_cases("1mb", "1MiB", "1048576", "128k")
    def test_get_blob_download(self, testcase):
        objects = self._put_objects(1, SMALL_FILE_SIZE)
        obj_names = list(objects.keys())
        evict_job_id = self.bucket.objects(obj_names=obj_names).evict()
        self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)

        for obj_name, content in objects.items():
            start_time = datetime.now().time()
            resp = (
                self.bucket.object(obj_name)
                .get(blob_chunk_size=testcase, blob_num_workers="4")
                .read_all()
            )
            self.assertEqual(content, resp)
            end_time = datetime.now().time()
            jobs_list = self.client.job(job_kind="blob-download").get_within_timeframe(
                start_time=start_time, end_time=end_time
            )
            self.assertTrue(len(jobs_list) > 0)

    @unittest.skipIf(
        "localhost" not in CLUSTER_ENDPOINT and "127.0.0.1" not in CLUSTER_ENDPOINT,
        "Cannot test promote without access to AIS cluster file storage",
    )
    # pylint: disable=too-many-locals
    def test_promote(self):
        self.bck_name = random_string()
        self.bucket = self._create_bucket(self.bck_name)
        self.local_test_files.mkdir()
        top_folder = self.local_test_files.joinpath("promote_folder")
        top_item = "test_file_top"
        top_item_contents = "contents in the test file"
        inner_folder_name = "inner_folder/"
        inner_item = "test_file_inner"
        inner_item_contents = "contents of the file in the inner folder"

        # Create a folder in the current directory
        local_files_path = Path().absolute().joinpath(top_folder)
        local_files_path.mkdir()
        with open(
            local_files_path.joinpath(top_item), "w", encoding=UTF_ENCODING
        ) as file:
            file.write(top_item_contents)
        inner_folder = local_files_path.joinpath(inner_folder_name)
        inner_folder.mkdir()
        with open(
            inner_folder.joinpath(inner_item), "w", encoding=UTF_ENCODING
        ) as file:
            file.write(inner_item_contents)

        # Promote to AIS bucket
        obj_name = "promoted_obj/"
        self.bucket.object(obj_name).promote(str(local_files_path))

        # Check bucket, only top object is promoted
        self.assertEqual(1, len(self.bucket.list_all_objects()))
        top_object = self.bucket.object(obj_name + top_item).get()
        self.assertEqual(top_item_contents, top_object.read_all().decode(UTF_ENCODING))

        # Update local top item contents
        top_item_updated_contents = "new content in top file overwritten"
        with open(
            local_files_path.joinpath(top_item), "w", encoding=UTF_ENCODING
        ) as file:
            file.write(top_item_updated_contents)

        # Promote with recursion, delete source, overwrite destination
        self.bucket.object(obj_name).promote(
            str(local_files_path),
            recursive=True,
            delete_source=True,
            overwrite_dest=True,
        )
        # Check bucket, both objects promoted, top overwritten
        self.assertEqual(2, len(self.bucket.list_all_objects()))
        expected_top_obj = obj_name + top_item
        top_obj = self.bucket.object(expected_top_obj).get()
        self.assertEqual(
            top_item_updated_contents, top_obj.read_all().decode(UTF_ENCODING)
        )
        inner_obj = self.bucket.object(obj_name + inner_folder_name + inner_item).get()
        self.assertEqual(inner_item_contents, inner_obj.read_all().decode(UTF_ENCODING))
        # Check source deleted
        top_level_files = [
            f
            for f in Path(top_folder).glob("*")
            if Path(top_folder).joinpath(f).is_file()
        ]
        self.assertEqual(0, len(top_level_files))
        self.assertEqual(0, len(list(inner_folder.glob("*"))))

    def test_delete(self):
        self.bck_name = random_string()
        self.bucket = self._create_bucket(self.bck_name)
        bucket_size = 10
        delete_cnt = 7

        obj_names = [f"obj-{ obj_id }" for obj_id in range(bucket_size)]
        self._create_objects(num_obj=bucket_size, obj_names=obj_names)
        objects = self.bucket.list_objects()
        self.assertEqual(len(objects.entries), bucket_size)

        for obj_id in range(delete_cnt):
            self.bucket.object(f"obj-{ obj_id + 1 }").delete()
        objects = self.bucket.list_objects()
        self.assertEqual(len(objects.entries), bucket_size - delete_cnt)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_blob_download(self):
        obj_name = "obj-blob-download"
        _ = self._create_object_with_content(obj_name=obj_name)

        evict_job_id = self.bucket.objects(obj_names=[obj_name]).evict()
        self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)

        blob_download_job_id = self.bucket.object(obj_name).blob_download()
        self.assertNotEqual(blob_download_job_id, "")
        self.client.job(job_id=blob_download_job_id).wait_single_node(
            timeout=TEST_TIMEOUT
        )

        objects = self.bucket.list_objects(props="name,cached", prefix=obj_name).entries
        self._validate_objects_cached(objects, True)
