import unittest
from unittest.mock import Mock, patch

from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    ACT_DELETE_OBJECTS,
    ACT_EVICT_OBJECTS,
    HTTP_METHOD_POST,
    ACT_PREFETCH_OBJECTS,
    ACT_COPY_OBJECTS,
    ACT_TRANSFORM_OBJECTS,
)
from aistore.sdk.etl_const import DEFAULT_ETL_TIMEOUT
from aistore.sdk.multiobj import ObjectGroup, ObjectRange
from aistore.sdk.types import BucketModel


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroup(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_bck = Mock()
        self.mock_response_text = "Response Text"
        mock_response = Mock()
        mock_response.text = self.mock_response_text
        self.mock_bck.make_request.return_value = mock_response

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

    def _copy_test_helper(self, starting_val, obj_group):
        to_bck = "to-bucket"
        expected_val = starting_val
        expected_val["prefix"] = ""
        expected_val["prepend"] = ""
        expected_val["dry_run"] = False
        expected_val["force"] = False
        expected_val["tobck"] = BucketModel(name=to_bck).as_dict()
        expected_val["coer"] = False
        # Test default args
        self.object_group_test_helper(
            obj_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            expected_val,
            to_bck=to_bck,
        )
        # Test provided optional args
        to_provider = "any provider"
        prepend_val = "new_prefix-"
        expected_val["prepend"] = prepend_val
        expected_val["force"] = True
        expected_val["dry_run"] = True
        expected_val["tobck"] = BucketModel(name=to_bck, provider=to_provider).as_dict()
        expected_val["coer"] = True
        self.object_group_test_helper(
            obj_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            expected_val,
            to_bck=to_bck,
            prepend=prepend_val,
            force=True,
            dry_run=True,
            to_provider=to_provider,
            continue_on_error=True,
        )

    def test_copy(self):
        self._copy_test_helper(self.expected_value, self.object_group)

    @patch("aistore.sdk.multiobj.object_group.logging")
    def test_copy_dry_run(self, mock_logging):
        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        self.object_group.copy(to_bck="to_bck", dry_run=True)

        mock_logger.info.assert_called()

    def _transform_test_helper(self, starting_val, obj_group):
        etl_name = "any active etl"
        to_bck = "to-bucket"
        expected_val = starting_val
        expected_val["prefix"] = ""
        expected_val["prepend"] = ""
        expected_val["dry_run"] = False
        expected_val["force"] = False
        expected_val["id"] = etl_name
        expected_val["request_timeout"] = DEFAULT_ETL_TIMEOUT
        expected_val["tobck"] = BucketModel(name=to_bck).as_dict()
        expected_val["coer"] = False
        # Test default args
        self.object_group_test_helper(
            obj_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            expected_val,
            to_bck=to_bck,
            etl_name=etl_name,
        )
        # Test provided optional args
        timeout = "30s"
        prepend_val = "new_prefix-"
        to_provider = "any provider"
        expected_val["tobck"] = BucketModel(name=to_bck, provider=to_provider).as_dict()
        expected_val["coer"] = True
        expected_val["prepend"] = prepend_val
        expected_val["request_timeout"] = timeout
        expected_val["dry_run"] = True
        expected_val["force"] = True
        self.object_group_test_helper(
            obj_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            expected_val,
            to_bck=to_bck,
            to_provider=to_provider,
            prepend=prepend_val,
            etl_name=etl_name,
            timeout=timeout,
            dry_run=True,
            force=True,
            continue_on_error=True,
        )

    def test_transform(self):
        self._transform_test_helper(self.expected_value, self.object_group)

    @patch("aistore.sdk.multiobj.object_group.logging")
    def test_transform_dry_run(self, mock_logging):
        mock_logger = Mock()
        mock_logging.getLogger.return_value = mock_logger

        self.object_group.transform(to_bck="to_bck", etl_name="any etl", dry_run=True)

        mock_logger.info.assert_called()

    def test_list_names(self):
        self.assertEqual(self.obj_names, self.object_group.list_names())
