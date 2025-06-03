import unittest
from unittest.mock import Mock, patch, call

from tests.const import LARGE_FILE_SIZE, ETL_NAME, PREFIX_NAME
from tests.utils import cases

from aistore.sdk import Bucket
from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    ACT_DELETE_OBJECTS,
    ACT_EVICT_OBJECTS,
    HTTP_METHOD_POST,
    ACT_PREFETCH_OBJECTS,
    ACT_COPY_OBJECTS,
    ACT_TRANSFORM_OBJECTS,
    ACT_ARCHIVE_OBJECTS,
    HTTP_METHOD_PUT,
)
from aistore.sdk.provider import Provider
from aistore.sdk.etl.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.multiobj import ObjectGroup, ObjectRange
from aistore.sdk.types import Namespace, BucketModel, ArchiveMultiObj
from aistore.sdk.etl import ETLConfig


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroup(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_bck = Mock()
        self.mock_bck.name = "mock-bucket"
        self.mock_bck.provider = "mock-bck-provider"
        self.mock_response_text = "Response Text"
        mock_response = Mock()
        mock_response.text = self.mock_response_text
        self.mock_bck.make_request.return_value = mock_response
        self.mock_bck_model = BucketModel(
            name=self.mock_bck.name, provider=self.mock_bck.provider
        )
        self.mock_bck.as_model.return_value = self.mock_bck_model
        namespace = Namespace(name="ns-name", uuid="ns-id")
        self.dest_bucket = Bucket(name="to-bucket", namespace=namespace)

        self.obj_names = ["obj-1", "obj-2"]
        self.object_group = ObjectGroup(self.mock_bck, obj_names=self.obj_names)
        self.expected_value = {}
        self.expected_value["objnames"] = self.obj_names

    def test_object_group_parameters(self):
        obj_names = ["list", "of", "names"]
        obj_range = ObjectRange(prefix=PREFIX_NAME)
        obj_template = "prefix-{0..3}"
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck,
                obj_names=obj_names,
                obj_range=obj_range,
            )
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck,
                obj_names=obj_names,
                obj_template=obj_template,
            )
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck,
                obj_range=obj_range,
                obj_template=obj_template,
            )

    # pylint: disable=too-many-arguments, too-many-positional-arguments
    def object_group_test_helper(
        self,
        object_group_function,
        http_method,
        action,
        expected_value,
        expect_list=False,
        **kwargs
    ):
        resp = object_group_function(**kwargs)
        if expect_list:
            # For copy and archive operations that return List[str]
            self.assertEqual([self.mock_response_text], resp)
        else:
            # For delete, evict, prefetch operations that return str
            self.assertEqual(self.mock_response_text, resp)
        self.mock_bck.make_request.assert_called_with(
            http_method,
            action,
            value=expected_value,
        )

    def test_delete(self):
        self.object_group_test_helper(
            self.object_group.delete,
            HTTP_METHOD_DELETE,
            ACT_DELETE_OBJECTS,
            self.expected_value,
        )

    def test_evict(self):
        self.object_group_test_helper(
            self.object_group.evict,
            HTTP_METHOD_DELETE,
            ACT_EVICT_OBJECTS,
            self.expected_value,
        )

    def test_prefetch(self):
        prefetch_expected_val = self.expected_value.copy()
        prefetch_expected_val["coer"] = False
        prefetch_expected_val["latest-ver"] = False
        prefetch_expected_val["num-workers"] = 3
        self.object_group_test_helper(
            self.object_group.prefetch,
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            prefetch_expected_val,
            num_workers=3,
        )

    def test_prefetch_with_blob_threshold(self):
        prefetch_expected_val = self.expected_value.copy()
        prefetch_expected_val["coer"] = False
        prefetch_expected_val["latest-ver"] = False
        blob_threshold_value = LARGE_FILE_SIZE
        prefetch_expected_val["blob-threshold"] = blob_threshold_value
        prefetch_expected_val["num-workers"] = 3

        self.object_group_test_helper(
            self.object_group.prefetch,
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            prefetch_expected_val,
            blob_threshold=blob_threshold_value,
            num_workers=3,
        )

    def test_copy(self):
        self.expected_value["prefix"] = ""
        self.expected_value["prepend"] = ""
        self.expected_value["dry_run"] = False
        self.expected_value["force"] = False
        self.expected_value["tobck"] = self.dest_bucket.as_model()
        self.expected_value["coer"] = False
        self.expected_value["latest-ver"] = False
        self.expected_value["synchronize"] = False
        # Test default args
        self.object_group_test_helper(
            self.object_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            self.expected_value,
            expect_list=True,
            to_bck=self.dest_bucket,
        )
        # Test provided optional args
        prepend_val = "new_prefix-"
        self.expected_value["prepend"] = prepend_val
        self.expected_value["force"] = True
        self.expected_value["dry_run"] = True
        self.expected_value["coer"] = True
        self.expected_value["latest-ver"] = False
        self.expected_value["synchronize"] = False
        self.expected_value["num-workers"] = 3

        self.object_group_test_helper(
            self.object_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            self.expected_value,
            expect_list=True,
            to_bck=self.dest_bucket,
            prepend=prepend_val,
            force=True,
            dry_run=True,
            continue_on_error=True,
            num_workers=3,
        )

    @patch("aistore.sdk.multiobj.object_group.logging")
    def test_copy_dry_run(self, mock_logging):
        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        self.object_group.copy(to_bck=self.dest_bucket, dry_run=True)

        mock_logger.info.assert_called()

    def test_transform(self):
        self.expected_value["prefix"] = ""
        self.expected_value["prepend"] = ""
        self.expected_value["dry_run"] = False
        self.expected_value["force"] = False
        self.expected_value["id"] = ETL_NAME
        self.expected_value["request_timeout"] = DEFAULT_ETL_TIMEOUT
        self.expected_value["tobck"] = self.dest_bucket.as_model()
        self.expected_value["coer"] = False
        self.expected_value["latest-ver"] = False
        self.expected_value["synchronize"] = False

        # Test default args
        self.object_group_test_helper(
            self.object_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            self.expected_value,
            to_bck=self.dest_bucket,
            etl_name=ETL_NAME,
        )
        # Test provided optional args
        timeout = "30s"
        prepend_val = "new_prefix-"
        self.expected_value["coer"] = True
        self.expected_value["prepend"] = prepend_val
        self.expected_value["ext"] = {"wav": "flac"}
        self.expected_value["request_timeout"] = timeout
        self.expected_value["dry_run"] = True
        self.expected_value["force"] = True
        self.expected_value["num-workers"] = 3
        self.object_group_test_helper(
            self.object_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            self.expected_value,
            to_bck=self.dest_bucket,
            prepend=prepend_val,
            ext={"wav": "flac"},
            etl_name=ETL_NAME,
            timeout=timeout,
            dry_run=True,
            force=True,
            continue_on_error=True,
            num_workers=3,
        )

    @patch("aistore.sdk.multiobj.object_group.logging")
    def test_transform_dry_run(self, mock_logging):
        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        self.object_group.transform(
            to_bck=self.dest_bucket, etl_name=ETL_NAME, dry_run=True
        )

        mock_logger.info.assert_called()

    def test_list_names(self):
        self.assertEqual(self.obj_names, self.object_group.list_names())

    def test_archive_default_params(self):
        archive_name = "test-arch"
        expected_value = ArchiveMultiObj(
            object_selection=self.expected_value,
            archive_name=archive_name,
            to_bck=self.mock_bck_model,
        ).as_dict()
        self.object_group_test_helper(
            self.object_group.archive,
            HTTP_METHOD_PUT,
            ACT_ARCHIVE_OBJECTS,
            expected_value=expected_value,
            expect_list=True,
            archive_name=archive_name,
        )

    def test_archive(self):
        archive_name = "test-arch"
        namespace = Namespace(name="ns-name", uuid="ns-id")
        to_bck = Bucket(
            name="dest-bck-name", namespace=namespace, provider=Provider.AMAZON
        )
        mime = "text"
        include_source = True
        allow_append = True
        continue_on_err = True
        expected_value = ArchiveMultiObj(
            object_selection=self.expected_value,
            archive_name=archive_name,
            to_bck=to_bck.as_model(),
            mime=mime,
            include_source_name=include_source,
            allow_append=allow_append,
            continue_on_err=continue_on_err,
        ).as_dict()
        self.object_group_test_helper(
            self.object_group.archive,
            HTTP_METHOD_PUT,
            ACT_ARCHIVE_OBJECTS,
            expected_value=expected_value,
            expect_list=True,
            archive_name=archive_name,
            to_bck=to_bck,
            mime=mime,
            include_source_name=include_source,
            allow_append=allow_append,
            continue_on_err=continue_on_err,
        )

    def test_list_urls(self):
        expected_obj_calls = []
        # Should create an object reference and get url for every object returned by listing
        for name in self.obj_names:
            expected_obj_calls.append(call(name))
            expected_obj_calls.append(call().get_url(etl=ETLConfig(name=ETL_NAME)))
        list(self.object_group.list_urls(etl=ETLConfig(name=ETL_NAME)))
        self.mock_bck.object.assert_has_calls(expected_obj_calls)

    def test_list_all_objects_iter(self):
        res = self.object_group.list_all_objects_iter(props=None)
        self.assertEqual(len(list(res)), len(self.obj_names))

    def test_prefixes(self):
        objs = list(self.object_group.list_all_objects_iter(prefix="obj"))
        self.assertEqual(len(objs), len(self.obj_names))

        objs = list(self.object_group.list_all_objects_iter(prefix="ojb"))
        self.assertEqual(len(objs), 0)

        objs = list(self.object_group.list_all_objects_iter(prefix="obj-1"))
        self.assertEqual(len(objs), 1)

    # pylint: disable=protected-access
    @cases(("uuid-1", ["uuid-1"]), ("uuid-1,uuid-2", ["uuid-1", "uuid-2"]))
    def test_parse_job_ids(self, case):
        job_ids, expected_result = case
        result = self.object_group._parse_job_ids(job_ids)
        self.assertEqual(result, expected_result)
