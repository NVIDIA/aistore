import unittest
from unittest.mock import Mock, patch, call

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
    PROVIDER_AMAZON,
    HTTP_METHOD_PUT,
)
from aistore.sdk.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.multiobj import ObjectGroup, ObjectRange
from aistore.sdk.types import Namespace, BucketModel, ArchiveMultiObj


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
        provider = "any provider"
        self.dest_bucket = Bucket(
            name="to-bucket", namespace=namespace, provider=provider
        )

        self.obj_names = ["obj-1", "obj-2"]
        self.object_group = ObjectGroup(self.mock_bck, obj_names=self.obj_names)
        self.expected_value = {"objnames": self.obj_names}

    def test_object_group_parameters(self):
        obj_names = ["list", "of", "names"]
        obj_range = ObjectRange(prefix="prefix-")
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

    def object_group_test_helper(
        self, object_group_function, http_method, action, expected_value, **kwargs
    ):
        resp_text = object_group_function(**kwargs)
        self.assertEqual(self.mock_response_text, resp_text)
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
        self.object_group_test_helper(
            self.object_group.prefetch,
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            self.expected_value,
        )

    def test_copy(self):
        self.expected_value["prefix"] = ""
        self.expected_value["prepend"] = ""
        self.expected_value["dry_run"] = False
        self.expected_value["force"] = False
        self.expected_value["tobck"] = self.dest_bucket.as_model()
        self.expected_value["coer"] = False
        # Test default args
        self.object_group_test_helper(
            self.object_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            self.expected_value,
            to_bck=self.dest_bucket,
        )
        # Test provided optional args
        prepend_val = "new_prefix-"
        self.expected_value["prepend"] = prepend_val
        self.expected_value["force"] = True
        self.expected_value["dry_run"] = True
        self.expected_value["coer"] = True
        self.object_group_test_helper(
            self.object_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            self.expected_value,
            to_bck=self.dest_bucket,
            prepend=prepend_val,
            force=True,
            dry_run=True,
            continue_on_error=True,
        )

    @patch("aistore.sdk.multiobj.object_group.logging")
    def test_copy_dry_run(self, mock_logging):
        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        self.object_group.copy(to_bck=self.dest_bucket, dry_run=True)

        mock_logger.info.assert_called()

    def test_transform(self):
        etl_name = "any active etl"
        self.expected_value["prefix"] = ""
        self.expected_value["prepend"] = ""
        self.expected_value["dry_run"] = False
        self.expected_value["force"] = False
        self.expected_value["id"] = etl_name
        self.expected_value["request_timeout"] = DEFAULT_ETL_TIMEOUT
        self.expected_value["tobck"] = self.dest_bucket.as_model()
        self.expected_value["coer"] = False
        # Test default args
        self.object_group_test_helper(
            self.object_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            self.expected_value,
            to_bck=self.dest_bucket,
            etl_name=etl_name,
        )
        # Test provided optional args
        timeout = "30s"
        prepend_val = "new_prefix-"
        self.expected_value["coer"] = True
        self.expected_value["prepend"] = prepend_val
        self.expected_value["request_timeout"] = timeout
        self.expected_value["dry_run"] = True
        self.expected_value["force"] = True
        self.object_group_test_helper(
            self.object_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            self.expected_value,
            to_bck=self.dest_bucket,
            prepend=prepend_val,
            etl_name=etl_name,
            timeout=timeout,
            dry_run=True,
            force=True,
            continue_on_error=True,
        )

    @patch("aistore.sdk.multiobj.object_group.logging")
    def test_transform_dry_run(self, mock_logging):
        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        self.object_group.transform(
            to_bck=self.dest_bucket, etl_name="any etl", dry_run=True
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
            archive_name=archive_name,
        )

    def test_archive(self):
        archive_name = "test-arch"
        namespace = Namespace(name="ns-name", uuid="ns-id")
        to_bck = Bucket(
            name="dest-bck-name", namespace=namespace, provider=PROVIDER_AMAZON
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
            archive_name=archive_name,
            to_bck=to_bck,
            mime=mime,
            include_source_name=include_source,
            allow_append=allow_append,
            continue_on_err=continue_on_err,
        )

    def test_get_urls(self):
        archpath = "myarch"
        etl_name = "myetl"
        expected_obj_calls = []
        # Should create an object reference and get url for every object returned by listing
        for name in self.obj_names:
            expected_obj_calls.append(call(name))
            expected_obj_calls.append(
                call().get_url(archpath=archpath, etl_name=etl_name)
            )
        list(self.object_group.get_urls(archpath=archpath, etl_name=etl_name))
        self.mock_bck.object.assert_has_calls(expected_obj_calls)
