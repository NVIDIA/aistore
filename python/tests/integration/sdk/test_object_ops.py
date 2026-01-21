#
# Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=duplicate-code

import random
import unittest
import io
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest
import xxhash

from aistore.sdk.blob_download_config import BlobDownloadConfig
from aistore.sdk.etl import ETLConfig
from aistore.sdk.errors import AISError
from aistore.sdk.const import (
    AIS_CUSTOM_MD,
    AIS_VERSION,
    HEADER_CONTENT_LENGTH,
    UTF_ENCODING,
)
from aistore import Client
from aistore.sdk.list_object_flag import ListObjectFlag
from aistore.sdk.archive_config import ArchiveMode, ArchiveConfig

from tests.const import (
    SMALL_FILE_SIZE,
    OBJ_READ_TYPE_ALL,
    OBJ_READ_TYPE_CHUNK,
    KIB,
    MIB,
)
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import (
    cases,
    create_archive,
    string_to_dict,
    has_targets,
    random_string,
)
from tests.const import TEST_TIMEOUT
from tests.integration import CLUSTER_ENDPOINT, REMOTE_SET, AWS_BUCKET


# pylint: disable=unused-variable, too-many-public-methods
class TestObjectOps(ParallelTestBase):
    def _test_get_obj(self, read_type, obj_name, exp_content):
        chunk_size = random.randrange(1, len(exp_content) + 10)
        reader = self.bucket.object(obj_name).get_reader(chunk_size=chunk_size)

        self.assertEqual(reader.attributes.size, len(exp_content))
        self.assertNotEqual(reader.attributes.checksum_type, "")
        self.assertNotEqual(reader.attributes.checksum_value, "")
        self.assertNotEqual(reader.attributes.access_time, "")
        if not REMOTE_SET:
            self.assertNotEqual(reader.attributes.obj_version, "")
            self.assertEqual(reader.attributes.custom_metadata, {})
        if read_type == OBJ_READ_TYPE_ALL:
            obj = reader.read_all()
        else:
            obj = b""
            for chunk in reader:
                obj += chunk
        self.assertEqual(obj, exp_content)

    def _put_objects(self, num_obj, obj_size=None):
        name_to_content = {}
        for i in range(num_obj):
            obj, content = self._create_object_with_content(obj_size=obj_size)
            name_to_content[obj.name] = content
        return name_to_content

    def test_put_content(self):
        obj, content = self._create_object_with_content()
        res = obj.get_reader()
        self.assertEqual(content, res.read_all())

    def test_put_file(self):
        content = b"content for the object"
        filename = self.local_test_files.joinpath("test_file")
        with open(filename, "wb") as writer:
            writer.write(content)
        obj = self._create_object()
        obj.get_writer().put_file(filename)
        res = obj.get_reader()
        self.assertEqual(content, res.read_all())

    def test_put_file_invalid(self):
        with self.assertRaises(ValueError):
            self.bucket.object("any").get_writer().put_file("non-existent-file")
        inner_dir = self.local_test_files.joinpath("inner_dir_not_file")
        inner_dir.mkdir()
        with self.assertRaises(ValueError):
            self.bucket.object("any").get_writer().put_file(inner_dir)

    def test_put_head_get(self):
        objects = self._put_objects(5)
        for obj_name, content in objects.items():
            properties = self.bucket.object(obj_name).head()
            if not REMOTE_SET:
                self.assertEqual(properties[AIS_VERSION], "1")
            self.assertEqual(properties[HEADER_CONTENT_LENGTH], str(len(content)))
            for option in [OBJ_READ_TYPE_ALL, OBJ_READ_TYPE_CHUNK]:
                self._test_get_obj(option, obj_name, content)

    def test_append_content(self):
        obj, content = self._create_object_with_content()

        obj_partitions = [b"1111111111", b"222222222222222", b"333333333"]
        next_handle = ""
        for data in obj_partitions:
            next_handle = obj.get_writer().append_content(data, next_handle)
        flushed = obj.get_writer().append_content(b"", next_handle, True)
        res = obj.get_reader()
        self.assertEqual(content + bytearray().join(obj_partitions), res.read_all())
        self.assertEqual(flushed, "")

    def test_get_object_appended_without_flush(self):
        obj, content = self._create_object_with_content()

        obj_partitions = [b"1111111111", b"222222222222222", b"333333333"]
        next_handle = ""
        for data in obj_partitions:
            next_handle = obj.get_writer().append_content(data, next_handle)
        res = obj.get_reader()
        self.assertEqual(content, res.read_all())

    def test_get_with_writer(self):
        filename = self.local_test_files.joinpath("test_get_with_writer.txt")
        objects = self._put_objects(10)
        all_content = b""
        for obj_name, content in objects.items():
            # Pass a writer that appends to a file
            with open(filename, "ab") as writer:
                self.bucket.object(obj_name).get_reader(writer=writer)
            all_content += content
        # Verify file contents are written from each object
        with open(filename, "rb") as reader:
            output = reader.read()
            self.assertEqual(all_content, output)
        filename.unlink()

    def test_get_range(self):
        objects = self._put_objects(5)
        for obj_name, content in objects.items():
            resp = (
                self.bucket.object(obj_name)
                .get_reader(byte_range="bytes=5-100")
                .read_all()
            )
            self.assertEqual(content[5:101], resp)

    def test_set_custom_props(self):
        obj, _ = self._create_object_with_content()

        obj.get_writer().set_custom_props(
            custom_metadata={"testkey1": "testval1", "testkey2": "testval2"}
        )
        self.assertTrue(
            {"testkey1": "testval1", "testkey2": "testval2"}.items()
            <= string_to_dict(obj.head()[AIS_CUSTOM_MD]).items()
        )

        obj.get_writer().set_custom_props(custom_metadata={"testkey3": "testval3"})
        self.assertTrue(
            {
                "testkey1": "testval1",
                "testkey2": "testval2",
                "testkey3": "testval3",
            }.items()
            <= string_to_dict(obj.head()[AIS_CUSTOM_MD]).items()
        )

        obj.get_writer().set_custom_props(
            custom_metadata={"testkey4": "testval4"}, replace_existing=True
        )
        self.assertTrue(
            {"testkey4": "testval4"}.items()
            <= string_to_dict(obj.head()[AIS_CUSTOM_MD]).items()
        )
        current_metadata = string_to_dict(obj.head()[AIS_CUSTOM_MD])
        self.assertNotIn("testkey1", current_metadata)
        self.assertNotIn("testkey2", current_metadata)
        self.assertNotIn("testkey3", current_metadata)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    @cases("1mb", "1MiB", "1048576", "128k")
    def test_get_blob_download(self, testcase):
        objects = self._put_objects(1, SMALL_FILE_SIZE)
        obj_names = list(objects.keys())
        evict_job_id = self.bucket.objects(obj_names=obj_names).evict()
        result = self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)

        for obj_name, content in objects.items():
            start_time = datetime.now(timezone.utc) - timedelta(seconds=1)
            blob_config = BlobDownloadConfig(chunk_size=testcase, num_workers="4")
            resp = (
                self.bucket.object(obj_name)
                .get_reader(blob_download_config=blob_config)
                .read_all()
            )
            self.assertEqual(content, resp)
            jobs_list = self.client.job(job_kind="blob-download").get_within_timeframe(
                start_time=start_time
            )
            self.assertTrue(len(jobs_list) > 0)

    @unittest.skipUnless(REMOTE_SET, "Remote bucket is not set")
    def test_obj_present(self):
        """
        Test the `Ais-Present` property of an object.
        This test ensures that the `present` property is correctly set for object.
        """
        obj, _ = self._create_object_with_content()

        # Verify the object is present
        self.assertEqual(
            True,
            obj.props.present,
            msg="The object should be present after putting content.",
        )

        # Evict the object
        evict_job_id = self.bucket.objects(obj_names=[obj.name]).evict()
        result = self.client.job(job_id=evict_job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)

        # Check the `Ais-Present` attribute after eviction
        # Note: `Ais-Present` should be "false" after eviction
        self.assertEqual(
            False,
            obj.props.present,
            msg="The object should not be present after eviction.",
        )

        # Get the entire object
        obj.get_reader().read_all()

        # Verify the object is present
        self.assertEqual(
            True,
            obj.props.present,
            msg="The object should be present after reading entire content.",
        )

    @unittest.skipIf(
        "localhost" not in CLUSTER_ENDPOINT and "127.0.0.1" not in CLUSTER_ENDPOINT,
        "Cannot test promote without access to AIS cluster file storage",
    )
    # pylint: disable=too-many-locals
    def test_promote(self):
        self.bucket = self._create_bucket()
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
        promote_job = self.bucket.object(obj_name).promote(str(local_files_path))

        # If promote is executed as an asynchronous job, wait until it completes
        if promote_job:
            result = self.client.job(job_id=promote_job).wait(timeout=TEST_TIMEOUT)
            self.assertTrue(result.success)

        # Check bucket, only top object is promoted
        self.assertEqual(1, len(self.bucket.list_all_objects()))
        top_object = self.bucket.object(obj_name + top_item).get_reader()
        self.assertEqual(top_item_contents, top_object.read_all().decode(UTF_ENCODING))

        # Update local top item contents
        top_item_updated_contents = "new content in top file overwritten"
        with open(
            local_files_path.joinpath(top_item), "w", encoding=UTF_ENCODING
        ) as file:
            file.write(top_item_updated_contents)

        # Promote with recursion, delete source, overwrite destination
        promote_job = self.bucket.object(obj_name).promote(
            str(local_files_path),
            recursive=True,
            delete_source=True,
            overwrite_dest=True,
        )

        # If promote is executed as an asynchronous job, wait until it completes
        if promote_job:
            result = self.client.job(job_id=promote_job).wait(timeout=TEST_TIMEOUT)
            self.assertTrue(result.success)

        # Check bucket, both objects promoted, top overwritten
        self.assertEqual(2, len(self.bucket.list_all_objects()))
        expected_top_obj = obj_name + top_item
        top_obj = self.bucket.object(expected_top_obj).get_reader()
        self.assertEqual(
            top_item_updated_contents, top_obj.read_all().decode(UTF_ENCODING)
        )
        inner_obj = self.bucket.object(
            obj_name + inner_folder_name + inner_item
        ).get_reader()
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
        self.bucket = self._create_bucket()
        bucket_size = 10
        delete_cnt = 7

        obj_names = list(self._create_objects(num_obj=bucket_size).keys())
        objects = self.bucket.list_objects()
        self.assertEqual(len(objects.entries), bucket_size)

        for obj_id in range(delete_cnt):
            self.bucket.object(obj_names[obj_id]).delete()
        objects = self.bucket.list_objects()
        self.assertEqual(len(objects.entries), bucket_size - delete_cnt)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_blob_download(self):
        obj, _ = self._create_object_with_content()

        evict_job_id = self.bucket.objects(obj_names=[obj.name]).evict()
        result = self.client.job(evict_job_id).wait(timeout=TEST_TIMEOUT)
        self.assertTrue(result.success)
        self.assertFalse(obj.props.present)

        blob_download_job_id = obj.blob_download()
        self.assertNotEqual(blob_download_job_id, "")
        result = self.client.job(job_id=blob_download_job_id).wait_single_node(
            timeout=TEST_TIMEOUT
        )
        self.assertTrue(result.success)
        self.assertTrue(obj.props.present)

    def test_get_archregex(self):
        archive_name = "test_archive.tar"
        archive_path = self.local_test_files.joinpath(archive_name)
        content_dict = {
            "file1.txt": b"Content of file one",
            "file2.txt": b"Content of file two",
            "file3.txt": b"Content of file three",
            "file1.cls": b"1",
            "file2.cls": b"2",
            "file3.cls": b"3",
        }
        create_archive(archive_path, content_dict)
        obj_name = f"{self.obj_prefix}-{archive_name}"
        obj = self.bucket.object(obj_name)
        obj.get_writer().put_file(archive_path)
        objs = self.bucket.list_objects_iter(
            prefix=obj.name, props="name", flags=[ListObjectFlag.ARCH_DIR]
        )
        self.assertEqual(len(list(objs)), 7)
        archive_config = ArchiveConfig(archpath="file1.txt")
        extracted_content_archpath = obj.get_reader(
            archive_config=archive_config
        ).read_all()
        self.assertEqual(extracted_content_archpath, content_dict["file1.txt"])

        # PREFIX Mode
        archive_config = ArchiveConfig(regex="file2", mode=ArchiveMode.PREFIX)
        extracted_content_regx = obj.get_reader(
            archive_config=archive_config
        ).read_all()
        file_like_object = io.BytesIO(extracted_content_regx)
        with tarfile.open(fileobj=file_like_object, mode="r:") as tar:
            self.assertEqual(tar.getnames(), ["file2.txt", "file2.cls"])
            file_content = tar.extractfile("file2.txt").read()
            self.assertEqual(file_content, content_dict["file2.txt"])
            file_content = tar.extractfile("file2.cls").read()
            self.assertEqual(file_content, content_dict["file2.cls"])

        # WDSKEY Mode
        archive_config = ArchiveConfig(regex="file3", mode=ArchiveMode.WDSKEY)
        extracted_content_regx = obj.get_reader(
            archive_config=archive_config
        ).read_all()
        file_like_object = io.BytesIO(extracted_content_regx)
        with tarfile.open(fileobj=file_like_object, mode="r:") as tar:
            self.assertEqual(tar.getnames(), ["file3.txt", "file3.cls"])
            file_content = tar.extractfile("file3.txt").read()
            self.assertEqual(file_content, content_dict["file3.txt"])
            file_content = tar.extractfile("file3.cls").read()
            self.assertEqual(file_content, content_dict["file3.cls"])

    def test_get_object_from_url(self):
        objects = self._put_objects(5)
        for obj_name, content in objects.items():
            url = f"{self.bucket.provider.value}://{self.bucket.name}/{obj_name}"
            fetched_obj = self.client.get_object_from_url(url)
            fetched_content = fetched_obj.get_reader().read_all()
            self.assertEqual(content, fetched_content)

    @unittest.skipIf(not has_targets(2), "Test requires more than one target")
    def test_get_object_direct(self):
        """
        Test fetching objects directly from the target node.
        """
        self.bucket = self._create_bucket()
        total_objects = 20
        obj_names = self._create_objects(num_obj=total_objects)

        for obj_name in obj_names:
            # Get object data directly from the target
            obj_from_direct = (
                self.bucket.object(obj_name).get_reader(direct=True).read_all()
            )
            self.assertIsNotNone(
                obj_from_direct, f"Direct fetch failed for object: {obj_name}"
            )

            # Get object data via proxy
            obj_from_non_direct = self.bucket.object(obj_name).get_reader().read_all()
            self.assertIsNotNone(
                obj_from_non_direct, f"Proxy fetch failed for object: {obj_name}"
            )

            # Verify direct and proxy data match
            self.assertEqual(
                obj_from_direct,
                obj_from_non_direct,
                f"Data mismatch for object: {obj_name}",
            )

            self.assertGreater(
                len(obj_from_direct), 0, f"Object data is empty for object: {obj_name}"
            )

    @unittest.skipIf(not has_targets(2), "Test requires more than one target")
    def test_get_object_direct_all_targets(self):
        """
        Test retrieving an object directly from all targets in the cluster.

        This test intentionally provides the client with incorrect target URLs to verify whether it can
        self-correct through retries. It simulates real-world scenarios where the smap changes,
        and the object may be relocated to a different target. The test ensures that the client can still
        successfully identify and connect to the correct target using `_retry_with_new_smap`.
        """
        self.bucket = self._create_bucket()
        obj, content = self._create_object_with_content()

        obj.get_writer().put_content(content)
        expected_content = obj.get_reader(direct=True).read_all()
        res = obj.get_reader()
        self.assertEqual(content, res.read_all())

        smap = self.client.cluster().get_info()

        for target in smap.tmap.values():
            if target.in_maint_or_decomm():
                continue
            clnt = Client(target.public_net.direct_url)
            content = (
                clnt.bucket(self.bucket.name)
                .object(obj.name)
                .get_reader(direct=True)
                .read_all()
            )
            self.assertEqual(content, expected_content)

    @cases(
        "%",
        "%25",
        "%3D",
        r"yHvMJM(s\(0hR\3\)",
        "file.mp3",
        "audio/raw/file.wav",
        "file_#@!$%^&*()_+.mp3",
        "track?name=lofi&v=1",
        "track%20space.wav",
        "my audio file .m4a",
        "你好世界",
        "-hiddenfile.ogg",
        ".audio_hidden.wav",
        "MiXeD_CaSe_Track.WAV",
        "track=id=1234.mp3",
    )
    def test_object_name_encoding(self, obj_name):
        """
        Test that object names with special characters are correctly encoded and decoded.
        """
        obj = self.bucket.object(obj_name)
        content = b"Special character test content"

        # Put object with special characters in name
        obj.get_writer().put_content(content)

        # Get object and verify content
        fetched_content = obj.get_reader().read_all()
        self.assertEqual(content, fetched_content)

        # Verify that the object can be listed with its special name
        listed_objects = self.bucket.list_objects_iter(prefix=obj_name)
        self.assertIn(obj_name, [o.name for o in listed_objects])
        obj.delete()

    def test_copy_object_same_name(self):
        """Test copying an object to another bucket with the same name."""
        dest_bucket = self._create_bucket(prefix="copy-dest-bucket")
        source_obj, content = self._create_object_with_content()

        dest_obj = dest_bucket.object(source_obj.name)
        response = source_obj.copy(dest_obj)
        self.assertEqual(response.status_code, 200)

        copied_content = dest_obj.get_reader().read_all()
        self.assertEqual(content, copied_content)

        source_content = source_obj.get_reader().read_all()
        self.assertEqual(content, source_content)

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_copy_object_latest_flag(self):
        """Test copying an object with latest flag to get updated version from remote backend."""
        dest_bucket = self._create_bucket(prefix="copy-latest-dest")
        obj_name = random_string()
        first_version_content = b"first version content"
        second_version_content = b"second version content"
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)

        # out-of-band PUT: first version
        self.s3_client.put_object(
            Bucket=self.bucket.name, Key=obj_name, Body=first_version_content
        )

        # cold GET to cache the object
        source_obj = self.bucket.object(obj_name)
        content = source_obj.get_reader().read_all()
        self.assertEqual(first_version_content, content)

        # out-of-band PUT: 2nd version (overwrite)
        self.s3_client.put_object(
            Bucket=self.bucket.name, Key=obj_name, Body=second_version_content
        )

        # Copy without latest flag - should copy the cached first version
        dest_obj_old = dest_bucket.object(f"{obj_name}-old")
        response = source_obj.copy(dest_obj_old)
        self.assertEqual(response.status_code, 200)

        # Verify copied content is the old cached version
        copied_content_old = dest_obj_old.get_reader().read_all()
        self.assertEqual(first_version_content, copied_content_old)

        # Copy with latest=True - should copy the updated version from remote
        dest_obj_latest = dest_bucket.object(f"{obj_name}-latest")
        response = source_obj.copy(dest_obj_latest, latest=True)
        self.assertEqual(response.status_code, 200)

        # Verify copied content is the updated version
        copied_content_latest = dest_obj_latest.get_reader().read_all()
        self.assertEqual(second_version_content, copied_content_latest)

        # out-of-band DELETE
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # Copy without latest should still work (using cached version)
        dest_obj_cached = dest_bucket.object(f"{obj_name}-cached")
        response = source_obj.copy(dest_obj_cached)
        self.assertEqual(response.status_code, 200)

        copied_content_cached = dest_obj_cached.get_reader().read_all()
        self.assertEqual(first_version_content, copied_content_cached)

        # Copy with latest=True should fail since object was deleted from remote
        dest_obj_deleted = dest_bucket.object(f"{obj_name}-deleted")
        with self.assertRaises(AISError):
            source_obj.copy(dest_obj_deleted, latest=True)

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_copy_object_sync_flag(self):
        """Test copying objects with sync flag to ensure synchronization with remote backend."""
        dest_bucket = self._create_bucket(prefix="copy-sync-dest")
        obj_name = random_string()
        content = b"sync test content"
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)

        # Create object out-of-band via S3
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=content)

        # Cache object by reading it
        source_obj = self.bucket.object(obj_name)
        cached_content = source_obj.get_reader().read_all()
        self.assertEqual(content, cached_content)

        # Copy without sync should work (uses cached version)
        dest_obj_no_sync = dest_bucket.object(f"no-sync-{obj_name}")
        response = source_obj.copy(dest_obj_no_sync)
        self.assertEqual(response.status_code, 200)

        # Verify copy worked
        copied_content = dest_obj_no_sync.get_reader().read_all()
        self.assertEqual(content, copied_content)

        # Delete object out-of-band via S3
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # Copy without sync should still work (uses cached version)
        dest_obj_cached = dest_bucket.object(f"cached-{obj_name}")
        response = source_obj.copy(dest_obj_cached)
        self.assertEqual(response.status_code, 200)

        # Copy with sync=True should fail since object was deleted from remote
        dest_obj_sync = dest_bucket.object(f"sync-{obj_name}")
        with self.assertRaises(AISError):
            source_obj.copy(dest_obj_sync, sync=True)

    def test_copy_object_different_name(self):
        """Test copying an object to another bucket with a different name."""
        dest_bucket = self._create_bucket(prefix="copy-dest-bucket")
        source_obj, content = self._create_object_with_content()

        new_name = f"copied-{source_obj.name}"
        dest_obj = dest_bucket.object(new_name)
        response = source_obj.copy(dest_obj)
        self.assertEqual(response.status_code, 200)

        copied_content = dest_obj.get_reader().read_all()
        self.assertEqual(content, copied_content)

        original_name_objs = list(dest_bucket.list_objects_iter(prefix=source_obj.name))
        original_name_matches = [
            obj for obj in original_name_objs if obj.name == source_obj.name
        ]
        self.assertEqual(len(original_name_matches), 0)

    def test_copy_object_same_bucket(self):
        """Test copying an object within the same bucket with a different name."""
        source_obj, content = self._create_object_with_content()

        new_name = f"copy-of-{source_obj.name}"
        dest_obj = self.bucket.object(new_name)
        response = source_obj.copy(dest_obj)
        self.assertEqual(response.status_code, 200)

        copied_content = dest_obj.get_reader().read_all()
        self.assertEqual(content, copied_content)

        source_content = source_obj.get_reader().read_all()
        self.assertEqual(content, source_content)

        all_objects = list(self.bucket.list_objects_iter())
        object_names = [obj.name for obj in all_objects]
        self.assertIn(source_obj.name, object_names)
        self.assertIn(new_name, object_names)

    def test_copy_multiple_objects_sequence(self):
        """Test copying multiple objects in sequence to verify no conflicts."""
        dest_bucket = self._create_bucket(prefix="copy-multi-dest")
        objects = self._put_objects(3)

        # Copy all objects
        for obj_name, content in objects.items():
            source_obj = self.bucket.object(obj_name)
            dest_obj = dest_bucket.object(obj_name)
            response = source_obj.copy(dest_obj)
            self.assertEqual(response.status_code, 200)

        # Verify all objects were copied correctly
        for obj_name, expected_content in objects.items():
            copied_obj = dest_bucket.object(obj_name)
            copied_content = copied_obj.get_reader().read_all()
            self.assertEqual(expected_content, copied_content)

        # Verify destination bucket has correct number of objects
        dest_objects = list(dest_bucket.list_objects_iter())
        self.assertEqual(len(dest_objects), len(objects))

    @pytest.mark.etl
    def test_copy_object_with_etl(self):
        """Test copying an object with ETL transformation."""
        dest_bucket = self._create_bucket(prefix="copy-etl-dest")
        source_obj, content = self._create_object_with_content()

        # Create ETL for MD5 transformation
        etl_name = "etl-copy-test"
        etl = self.client.etl(etl_name)

        try:
            etl.init(image="aistorage/transformer_hash_with_args:latest")

            # Copy with ETL transformation
            dest_obj = dest_bucket.object(source_obj.name)
            etl_config = ETLConfig(name=etl_name, args="123")
            response = source_obj.copy(dest_obj, etl=etl_config)
            self.assertIn(response.status_code, [200, 204])

            # Verify the copied object has the transformed content (MD5 hash)
            copied_obj = dest_bucket.object(source_obj.name)
            copied_content = copied_obj.get_reader().read_all()

            hasher = xxhash.xxh64(seed=int(etl_config.args))
            hasher.update(content)
            self.assertEqual(copied_content, hasher.hexdigest().encode("ascii"))

            # Verify original object is unchanged
            original_content = source_obj.get_reader().read_all()
            self.assertEqual(original_content, content)

        finally:
            try:  # pylint: disable=duplicate-code
                etl.stop()
                etl.delete()
            except AISError:
                pass

    def test_copy_object_to_nonexistent_bucket(self):
        """Test copying an object to a non-existent bucket raises an error."""
        source_obj, _ = self._create_object_with_content()

        # Use a random bucket name to avoid conflicts
        bucket_name = f"nonexistent-bucket-{random_string(8)}"
        nonexistent_bucket = self.client.bucket(bucket_name)

        dest_obj = nonexistent_bucket.object("test-object")

        # Copy should raise an error
        with self.assertRaises(AISError) as context:
            source_obj.copy(dest_obj)

        # Verify error content from response body
        self.assertIn("bucket", context.exception.message.lower())
        self.assertIn("does not exist", context.exception.message.lower())

    def test_head_v2_basic(self):
        """Test head_v2() returns V2 attributes with basic properties."""
        obj, content = self._create_object_with_content()

        attrs = obj.head_v2("checksum")

        # V2 should return size and standard attributes
        self.assertEqual(len(content), attrs.size)
        self.assertNotEqual("", attrs.checksum_type)
        self.assertNotEqual("", attrs.checksum_value)

    def test_head_v2_chunked_props(self):
        """Test head_v2() with 'chunked' props returns chunk info or None."""
        obj, _ = self._create_object_with_content()

        attrs = obj.head_v2(props="chunked")

        # For regular (non-chunked) objects, chunks should be None
        # This test ensures the API path works correctly
        self.assertTrue(hasattr(attrs, "chunks"))

    def test_head_v2_last_modified_and_etag(self):
        """Test head_v2() returns last_modified and etag only when requested."""
        obj, _ = self._create_object_with_content()

        # Without requesting, should be empty
        attrs_default = obj.head_v2()
        self.assertEqual("", attrs_default.last_modified)
        self.assertEqual("", attrs_default.etag)

        # With last-modified requested
        attrs_lm = obj.head_v2(props="last-modified")
        self.assertNotEqual("", attrs_lm.last_modified)

        # With etag requested
        attrs_etag = obj.head_v2(props="etag")
        self.assertNotEqual("", attrs_etag.etag)
        self.assertNotIn('"', attrs_etag.etag)  # quotes should be stripped

        # With both requested
        attrs_both = obj.head_v2(props="last-modified,etag")
        self.assertNotEqual("", attrs_both.last_modified)
        self.assertNotEqual("", attrs_both.etag)

    def test_head_v2_multipart_chunk_info(self):
        """Test head_v2() returns correct chunk info for multipart uploaded objects."""
        obj = self._create_object()

        # Create multipart upload with known part sizes
        mpu = obj.multipart_upload().create()

        part_size = 5 * MIB  # 5 MiB per part
        num_parts = 3
        for i in range(1, num_parts + 1):
            content = b"x" * part_size
            mpu.add_part(i).put_content(content)

        mpu.complete()

        # Verify head_v2 returns correct chunk info
        attrs = obj.head_v2(props="chunked")

        self.assertIsNotNone(attrs.chunks)
        self.assertEqual(num_parts, attrs.chunks.chunk_count)
        self.assertEqual(part_size, attrs.chunks.max_chunk_size)
        self.assertEqual(part_size * num_parts, attrs.size)

    @cases(2, 4, 8)
    def test_parallel_download_various_workers(self, num_workers):
        """Test parallel download with various worker counts."""
        content = b"x" * (100 * KIB)
        obj = self._create_object()
        obj.get_writer().put_content(content)

        reader = obj.get_reader(num_workers=num_workers)
        result = b"".join(reader)

        self.assertEqual(content, result)
