#
# Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
#
import random
import unittest
from pathlib import Path

from aistore.sdk.const import AIS_VERSION, HEADER_CONTENT_LENGTH, UTF_ENCODING

from aistore.sdk import Client
from tests.utils import (
    create_and_put_object,
    random_string,
    destroy_bucket,
    cleanup_local,
)
from tests.integration import CLUSTER_ENDPOINT

OBJ_READ_TYPE_ALL = "read_all"
OBJ_READ_TYPE_CHUNK = "chunk"
OBJ_NAME = "test-object"


# pylint: disable=unused-variable
class TestObjectOps(unittest.TestCase):
    def setUp(self) -> None:
        self.bck_name = random_string()

        self.client = Client(CLUSTER_ENDPOINT)
        self.bucket = self.client.bucket(self.bck_name)
        self.bucket.create()
        self.local_test_files = (
            Path().absolute().joinpath("object-ops-test-" + random_string(8))
        )

    def tearDown(self) -> None:
        # Try to destroy bucket if there is one left.
        destroy_bucket(self.client, self.bck_name)
        # Cleanup local files at end
        cleanup_local(str(self.local_test_files))

    def _test_get_obj(self, read_type, obj_name, exp_content):
        chunk_size = random.randrange(1, len(exp_content) + 10)
        stream = self.bucket.object(obj_name).get(chunk_size=chunk_size)

        self.assertEqual(stream.attributes.size, len(exp_content))
        self.assertNotEqual(stream.attributes.checksum_type, "")
        self.assertNotEqual(stream.attributes.checksum_value, "")
        self.assertNotEqual(stream.attributes.access_time, "")
        self.assertNotEqual(stream.attributes.obj_version, "")
        self.assertEqual(stream.attributes.custom_metadata, {})
        if read_type == OBJ_READ_TYPE_ALL:
            obj = stream.read_all()
        else:
            obj = b""
            for chunk in stream:
                obj += chunk
        self.assertEqual(obj, exp_content)

    def _put_objects(self, num_obj):
        name_to_content = {}
        for i in range(num_obj):
            obj_name = f"obj{ i }"
            content = create_and_put_object(
                client=self.client, bck_name=self.bck_name, obj_name=obj_name
            )
            name_to_content[obj_name] = content
        return name_to_content

    def test_put_content(self):
        content = b"content for the object"
        obj = self.bucket.object(OBJ_NAME)
        obj.put_content(content)
        res = obj.get()
        self.assertEqual(content, res.read_all())

    def test_put_file(self):
        self.local_test_files.mkdir()
        content = b"content for the object"
        filename = self.local_test_files.joinpath("test_file")
        with open(filename, "wb") as writer:
            writer.write(content)
        obj = self.bucket.object(OBJ_NAME)
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

    @unittest.skipIf(
        "localhost" not in CLUSTER_ENDPOINT and "127.0.0.1" not in CLUSTER_ENDPOINT,
        "Cannot test promote without access to AIS cluster file storage",
    )
    # pylint: disable=too-many-locals
    def test_promote(self):
        self.local_test_files.mkdir()
        top_folder = self.local_test_files.joinpath("promote_folder")
        top_item = "test_file_top"
        top_item_contents = "contents in the test file"
        inner_folder = "inner_folder"
        inner_item = "test_file_inner"
        inner_item_contents = "contents of the file in the inner folder"
        # Create a folder in the current directory
        local_files_path = Path().absolute().joinpath(top_folder)
        local_files_path.mkdir()
        with open(
            local_files_path.joinpath(top_item), "w", encoding=UTF_ENCODING
        ) as file:
            file.write(top_item_contents)
        inner_folder = local_files_path.joinpath(inner_folder)
        inner_folder.mkdir()
        with open(
            inner_folder.joinpath(inner_item), "w", encoding=UTF_ENCODING
        ) as file:
            file.write(inner_item_contents)

        # Promote to AIS bucket
        obj_name = "promoted_obj"
        self.bucket.object(obj_name).promote(str(local_files_path))
        # Check bucket, only top object is promoted
        self.assertEqual(1, len(self.bucket.list_all_objects()))
        top_object = self.bucket.object(obj_name + "/" + top_item).get()
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
        expected_top_obj = obj_name + "/" + top_item
        top_obj = self.bucket.object(expected_top_obj).get()
        self.assertEqual(
            top_item_updated_contents, top_obj.read_all().decode(UTF_ENCODING)
        )
        inner_obj = self.bucket.object(obj_name + "/inner_folder/" + inner_item).get()
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
        bucket_size = 10
        delete_cnt = 7

        for obj_id in range(bucket_size):
            create_and_put_object(
                self.client, bck_name=self.bck_name, obj_name=f"obj-{ obj_id }"
            )

        objects = self.bucket.list_objects()
        self.assertEqual(len(objects.get_entries()), bucket_size)

        for obj_id in range(delete_cnt):
            self.bucket.object(f"obj-{ obj_id + 1 }").delete()
        objects = self.bucket.list_objects()
        self.assertEqual(len(objects.get_entries()), bucket_size - delete_cnt)
