#
# Copyright (c) 2023-2025, NVIDIA CORPORATION. All rights reserved.
#
import hashlib
import unittest
import tarfile
import io
from datetime import datetime, timedelta, timezone

import pytest

from aistore.sdk.const import LOREM, DUIS
from aistore.sdk.provider import Provider
from aistore.sdk.errors import InvalidBckProvider, AISError, JobInfoNotFound
from aistore.sdk.etl.webserver.http_multi_threaded_server import HTTPMultiThreadedServer
from tests.const import (
    MEDIUM_FILE_SIZE,
    OBJECT_COUNT,
    TEST_TIMEOUT,
    TEST_TIMEOUT_LONG,
    PREFIX_NAME,
    SUFFIX_NAME,
    SMALL_FILE_SIZE,
)
from tests.integration import REMOTE_SET, AWS_BUCKET
from tests.integration.sdk.parallel_test_base import ParallelTestBase
from tests.utils import random_string, assert_with_retries


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroupOps(ParallelTestBase):
    def setUp(self) -> None:
        super().setUp()
        self.suffix = SUFFIX_NAME
        self.obj_extension = self.suffix[-3:]
        # Use a slightly larger file size to allow for blob threshold (must be > 1MiB)
        self.file_size = MEDIUM_FILE_SIZE
        self.obj_names = self._create_objects(
            suffix=self.suffix, obj_size=self.file_size
        )

    def test_delete(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        job_id = object_group.delete()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 2)
        existing_objects = self.bucket.list_objects(prefix=self.obj_prefix).entries
        self.assertEqual(1, len(existing_objects))
        self.assertEqual(self.obj_names[0], existing_objects[0].name)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_evict(self):
        object_group = self.bucket.objects(obj_names=self.obj_names[1:])
        job_id = object_group.evict()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 2)
        self._verify_cached_objects(OBJECT_COUNT, [0])

    def test_evict_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=Provider.AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).evict()

    def _prefetch_objects_test_helper(self, num_workers=None):
        objects_excluded = 1
        all_objects = self.bucket.objects(obj_names=self.obj_names)
        # Evict and verify
        self._evict_objects(all_objects)

        # Fetch back a specific object group
        prefetched_objects = self.bucket.objects(
            obj_names=self.obj_names[objects_excluded:]
        )
        prefetch_kwargs = {}
        if num_workers is not None:
            prefetch_kwargs["num_workers"] = num_workers
        job_id = prefetched_objects.prefetch(**prefetch_kwargs)
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 5)

        # Verify all objects exist but only those in the prefetch group are now cached
        self._verify_cached_objects(OBJECT_COUNT, range(objects_excluded, OBJECT_COUNT))

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_list(self):
        # Use smaller size to help speed with lots of remote objects
        self.file_size = SMALL_FILE_SIZE
        self._prefetch_objects_test_helper()

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_list_with_num_workers(self):
        self.file_size = SMALL_FILE_SIZE
        self._prefetch_objects_test_helper(num_workers=3)

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    def test_prefetch_blob_download(self):
        obj_group = self.bucket.objects(obj_names=self.obj_names)
        self._evict_objects(obj_group)
        start_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        # Use a threshold that's just low enough for our object size to require blob
        job_id = obj_group.prefetch(blob_threshold=self.file_size)
        self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT * 5)

        jobs_list = self.client.job(job_kind="blob-download").get_within_timeframe(
            start_time=start_time
        )
        filtered_jobs = [
            job for job in jobs_list if job.bucket.name == self.bucket.name
        ]

        self.assertTrue(len(filtered_jobs) > 0)
        self._check_all_objects_cached(
            len(obj_group.list_names()), expected_cached=True
        )

    @unittest.skipIf(
        not REMOTE_SET,
        "Remote bucket is not set",
    )
    @pytest.mark.nonparallel("checks job within timeframe")
    def test_prefetch_without_blob_download(self):
        obj_group = self.bucket.objects(obj_names=self.obj_names)
        self._evict_objects(obj_group)
        start_time = datetime.now(timezone.utc) - timedelta(seconds=1)
        job_id = obj_group.prefetch(blob_threshold=self.file_size + 1)
        self.client.job(job_id=job_id).wait(timeout=TEST_TIMEOUT * 5)

        with self.assertRaises(JobInfoNotFound):
            self.client.job(job_kind="blob-download").get_within_timeframe(
                start_time=start_time
            )
        self._check_all_objects_cached(
            len(obj_group.list_names()), expected_cached=True
        )

    def test_prefetch_objects_local(self):
        local_bucket = self.client.bucket(random_string(), provider=Provider.AIS)
        with self.assertRaises(InvalidBckProvider):
            local_bucket.objects(obj_names=[]).prefetch()

    def _copy_objects_test_helper(self, num_workers=None):
        to_bck = self._create_bucket()
        self.assertEqual(0, len(to_bck.list_all_objects(prefix=self.obj_prefix)))
        self.assertEqual(
            OBJECT_COUNT, len(self.bucket.list_all_objects(prefix=self.obj_prefix))
        )

        new_prefix = PREFIX_NAME
        copy_kwargs = {"prepend": new_prefix}
        if num_workers is not None:
            copy_kwargs["num_workers"] = num_workers

        copy_job_ids = self.bucket.objects(obj_names=self.obj_names[1:5]).copy(
            to_bck, **copy_kwargs
        )
        for job_id in copy_job_ids:
            self.client.job(job_id=job_id).wait_for_idle(timeout=TEST_TIMEOUT * 2)

        self.assertEqual(
            4, len(to_bck.list_all_objects(prefix=new_prefix + self.obj_prefix))
        )

    def test_copy_objects(self):
        self._copy_objects_test_helper()

    def test_copy_objects_with_num_workers(self):
        self._copy_objects_test_helper(num_workers=3)

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_copy_objects_latest_flag(self):
        obj_name = random_string()
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)
        to_bck = self._create_bucket()

        # out-of-band PUT: first version
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=LOREM)

        # copy, and check
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, LOREM, False)
        # create a cached copy in src bucket
        content = self.bucket.object(obj_name).get_reader().read_all()
        self.assertEqual(LOREM, content.decode("utf-8"))

        # out-of-band PUT: 2nd version (overwrite)
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=DUIS)

        # copy and check (expecting the first version)
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, LOREM, False)

        # copy latest: update in-cluster copy
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, DUIS, True)
        # check if cached copy is src bck is still on prev version
        content = self.bucket.object(obj_name).get_reader().read_all()
        self.assertEqual(LOREM, content.decode("utf-8"))

        # out-of-band DELETE
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # copy and check (expecting no changes)
        self._copy_and_check_with_latest(self.bucket, to_bck, obj_name, DUIS, True)

        # run copy with '--sync' one last time, and make sure the object "disappears"
        copy_job_ids = self.bucket.objects(obj_names=[obj_name]).copy(
            self.bucket, sync=True
        )
        for job_id in copy_job_ids:
            self.client.job(job_id=job_id).wait_for_idle(timeout=TEST_TIMEOUT * 2)
        with self.assertRaises(AISError):
            self.bucket.object(obj_name).get_reader().read_all()

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    @pytest.mark.nonparallel("job uuid query does not work with multiple")
    def test_object_group_copy_sync_flag(self):
        to_bck = self._create_bucket()

        # run copy with '--sync' on different dst, and make sure the object "disappears"
        # multi-obj --sync currently only supports templates
        # TODO: add test for multi-obj list --sync once api is ready
        template = self.obj_prefix + "{0..10}" + self.suffix
        obj_group = self.bucket.objects(obj_template=template)
        copy_job_ids = obj_group.copy(to_bck)
        for job_id in copy_job_ids:
            self.client.job(job_id=job_id).wait_for_idle(timeout=TEST_TIMEOUT * 2)
        self.assertEqual(
            len(to_bck.list_all_objects(prefix=self.obj_prefix)), OBJECT_COUNT
        )

        # out of band delete all objects
        for obj_name in self.obj_names:
            self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        copy_job_ids = self.bucket.objects(obj_template=template).copy(
            to_bck, sync=True
        )
        for job_id in copy_job_ids:
            self.client.job(job_id=job_id).wait_for_idle(timeout=TEST_TIMEOUT * 2)

        # NOTE: S3 and similar providers are only *eventually* consistent.
        #       Wrap emptiness assertions in a retry to avoid flakes.

        # check to see if all the objects in dst disappear after cp multi-obj sync
        assert_with_retries(
            self.assertEqual, 0, len(to_bck.list_all_objects(prefix=self.obj_prefix))
        )
        # objects also disappear from src bck
        assert_with_retries(
            self.assertEqual,
            0,
            len(self.bucket.list_all_objects(prefix=self.obj_prefix)),
        )

    @unittest.skipIf(
        not AWS_BUCKET,
        "AWS bucket is not set",
    )
    def test_prefetch_objects_latest_flag(self):
        obj_name = random_string()
        self._register_for_post_test_cleanup(names=[obj_name], is_bucket=False)

        # out-of-band PUT: first version
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=LOREM)

        # prefetch, and check
        self._prefetch_and_check_with_latest(self.bucket, obj_name, LOREM, False)

        # out-of-band PUT: 2nd version (overwrite)
        self.s3_client.put_object(Bucket=self.bucket.name, Key=obj_name, Body=DUIS)

        # prefetch and check (expecting the first version)
        self._prefetch_and_check_with_latest(self.bucket, obj_name, LOREM, False)

        # prefetch latest: update in-cluster copy
        self._prefetch_and_check_with_latest(self.bucket, obj_name, DUIS, True)

        # out-of-band DELETE
        self.s3_client.delete_object(Bucket=self.bucket.name, Key=obj_name)

        # prefetch without '--latest': expecting no changes
        self._prefetch_and_check_with_latest(self.bucket, obj_name, DUIS, False)

        # run prefetch with '--latest' one last time, and make sure the object "disappears"
        # prefetch_job = self.bucket.objects(obj_names=[obj_name]).prefetch(latest=True)
        # self.client.job(job_id=prefetch_job).wait_for_idle(timeout=TEST_TIMEOUT)
        # with self.assertRaises(AISError):
        #    self.bucket.object(obj_name).get_reader().read_all()

    def _prefetch_and_check_with_latest(self, bucket, obj_name, expected, latest_flag):
        prefetch_job = bucket.objects(obj_names=[obj_name]).prefetch(latest=latest_flag)
        self.client.job(job_id=prefetch_job).wait_for_idle(timeout=TEST_TIMEOUT * 2)

        content = bucket.object(obj_name).get_reader().read_all()
        self.assertEqual(expected, content.decode("utf-8"))

    # pylint: disable=too-many-arguments,too-many-positional-arguments
    def _copy_and_check_with_latest(
        self, from_bck, to_bck, obj_name, expected, latest_flag
    ):
        copy_job_ids = from_bck.objects(obj_names=[obj_name]).copy(
            to_bck, latest=latest_flag
        )
        for job_id in copy_job_ids:
            self.client.job(job_id=job_id).wait_for_idle(timeout=TEST_TIMEOUT * 2)
        self.assertEqual(1, len(to_bck.list_all_objects()))
        content = to_bck.object(obj_name).get_reader().read_all()
        self.assertEqual(expected, content.decode("utf-8"))

    def test_archive_objects_without_copy(self):
        arch_name = self.obj_prefix + "-archive-without-copy.tar"
        self._archive_exec_assert(arch_name, self.bucket, self.bucket)

    def test_archive_objects_with_copy(self):
        arch_name = self.obj_prefix + "-archive-with-copy.tar"
        dest_bck = self._create_bucket()
        self._archive_exec_assert(arch_name, self.bucket, dest_bck, to_bck=dest_bck)

    def _archive_exec_assert(self, arch_name, src_bck, res_bck, **kwargs):
        # Add to object list to clean up on test finish
        if res_bck.provider != Provider.AIS:
            self._register_for_post_test_cleanup(names=[arch_name], is_bucket=False)
        archived_names = self.obj_names[1:5]
        expected_contents = {}
        for name in archived_names:
            expected_contents[name] = (
                src_bck.object(obj_name=name).get_reader().read_all()
            )

        arch_job_ids = src_bck.objects(obj_names=archived_names).archive(
            archive_name=arch_name, **kwargs
        )
        for job_id in arch_job_ids:
            self.client.job(job_id=job_id).wait_for_idle(timeout=TEST_TIMEOUT * 2)

        # Read the tar archive and assert the object names and contents match
        res_bytes = res_bck.object(arch_name).get_reader().read_all()
        with tarfile.open(fileobj=io.BytesIO(res_bytes), mode="r") as tar:
            member_names = []
            for member in tar.getmembers():
                inner_file = tar.extractfile(member)
                self.assertEqual(expected_contents[member.name], inner_file.read())
                inner_file.close()
                member_names.append(member.name)
            self.assertEqual(set(archived_names), set(member_names))

    # pylint: disable=unused-argument, duplicate-code
    def _transform_objects_test_helper(self, num_workers=None):
        # Define an ETL that hashes the contents of each object
        etl_name = "etl-" + random_string(5)

        def transform(input_bytes):
            md5 = hashlib.md5()
            md5.update(input_bytes)
            return md5.hexdigest().encode()

        md5_etl = self.client.etl(etl_name)

        @md5_etl.init_class()
        class MD5Server(HTTPMultiThreadedServer):
            def transform(self, data: bytes, *_args) -> bytes:
                return hashlib.md5(data).hexdigest().encode()

        to_bck = self._create_bucket()
        new_prefix = PREFIX_NAME
        new_ext = "new-ext"
        self.assertEqual(0, len(to_bck.list_all_objects(prefix=self.obj_prefix)))
        self.assertEqual(
            OBJECT_COUNT, len(self.bucket.list_all_objects(prefix=self.obj_prefix))
        )

        transform_kwargs = {
            "to_bck": to_bck,
            "etl_name": md5_etl.name,
            "prepend": new_prefix,
            "ext": {self.obj_extension: new_ext},
        }
        if num_workers is not None:
            transform_kwargs["num_workers"] = num_workers

        transform_job = self.bucket.objects(obj_names=self.obj_names).transform(
            **transform_kwargs
        )
        self.client.job(job_id=transform_job).wait_for_idle(timeout=TEST_TIMEOUT_LONG)

        # Get the md5 transform of each source object and verify the destination bucket contains those results
        from_obj_hashes = [
            transform(self.bucket.object(name).get_reader().read_all())
            for name in self.obj_names
        ]
        to_obj_values = [
            to_bck.object(self._new_name(name, new_prefix, new_ext))
            .get_reader()
            .read_all()
            for name in self.obj_names
        ]
        self.assertEqual(to_obj_values, from_obj_hashes)

    @pytest.mark.etl
    @unittest.skipIf(
        REMOTE_SET,
        "TODO -- FIXME: Remote bucket transform",
    )
    def test_transform_objects(self):
        self._transform_objects_test_helper()

    @pytest.mark.etl
    @unittest.skipIf(
        REMOTE_SET,
        "TODO -- FIXME: Remote bucket transform",
    )
    def test_transform_objects_with_num_workers(self):
        self._transform_objects_test_helper(num_workers=3)

    def _evict_objects(self, obj_group):
        job_id = obj_group.evict()
        self.client.job(job_id).wait(timeout=TEST_TIMEOUT * 2)
        self._check_all_objects_cached(
            len(obj_group.list_names()), expected_cached=False
        )

    def _new_name(self, s, prefix, ext):
        return prefix + s.rstrip(self.obj_extension) + ext
