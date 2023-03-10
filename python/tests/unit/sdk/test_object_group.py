import unittest
from unittest.mock import Mock

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
from aistore.sdk.object_group import ObjectGroup
from aistore.sdk.object_range import ObjectRange
from aistore.sdk.types import BucketModel


# pylint: disable=unused-variable,too-many-instance-attributes
class TestObjectGroup(unittest.TestCase):
    def setUp(self) -> None:
        self.mock_bck = Mock()
        self.mock_response_text = "Response Text"
        mock_response = Mock()
        mock_response.text = self.mock_response_text
        self.mock_bck.make_request.return_value = mock_response

        self.obj_prefix = "prefix-"
        self.obj_suffix = "-suffix"
        self.obj_range = ObjectRange(
            self.obj_prefix, 1, 8, step=2, suffix=self.obj_suffix
        )
        self.obj_range_template = "prefix-{1..8..2}-gap-{12..15..1}-suffix"
        self.obj_names = ["obj-1", "obj-2"]
        self.object_group_name_list = ObjectGroup(
            self.mock_bck, obj_names=self.obj_names
        )
        self.object_group_range = ObjectGroup(self.mock_bck, obj_range=self.obj_range)
        self.object_group_template = ObjectGroup(
            self.mock_bck, obj_template=self.obj_range_template
        )
        self.expected_name_list_value = {"objnames": self.obj_names}
        self.expected_range_value = {"template": str(self.obj_range)}
        self.expected_range_template_value = {"template": self.obj_range_template}

    def test_object_group_parameters(self):
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck,
                obj_names=self.obj_names,
                obj_range=self.obj_range,
            )
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck,
                obj_names=self.obj_names,
                obj_template=self.obj_range_template,
            )
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck,
                obj_range=self.obj_range,
                obj_template=self.obj_range_template,
            )

    def test_delete_names(self):
        self.object_group_test_helper(
            self.object_group_name_list.delete,
            HTTP_METHOD_DELETE,
            ACT_DELETE_OBJECTS,
            self.expected_name_list_value,
        )

    def test_delete_range(self):
        self.object_group_test_helper(
            self.object_group_range.delete,
            HTTP_METHOD_DELETE,
            ACT_DELETE_OBJECTS,
            self.expected_range_value,
        )

    def test_delete_range_list(self):
        self.object_group_test_helper(
            self.object_group_template.delete,
            HTTP_METHOD_DELETE,
            ACT_DELETE_OBJECTS,
            self.expected_range_template_value,
        )

    def test_evict_names(self):
        self.object_group_test_helper(
            self.object_group_name_list.evict,
            HTTP_METHOD_DELETE,
            ACT_EVICT_OBJECTS,
            self.expected_name_list_value,
        )

    def test_evict_range(self):
        self.object_group_test_helper(
            self.object_group_range.evict,
            HTTP_METHOD_DELETE,
            ACT_EVICT_OBJECTS,
            self.expected_range_value,
        )

    def test_evict_range_list(self):
        self.object_group_test_helper(
            self.object_group_template.evict,
            HTTP_METHOD_DELETE,
            ACT_EVICT_OBJECTS,
            self.expected_range_template_value,
        )

    def test_prefetch_names(self):
        self.object_group_test_helper(
            self.object_group_name_list.prefetch,
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            self.expected_name_list_value,
        )

    def test_prefetch_range(self):
        self.object_group_test_helper(
            self.object_group_range.prefetch,
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            self.expected_range_value,
        )

    def test_prefetch_range_template(self):
        self.object_group_test_helper(
            self.object_group_template.prefetch,
            HTTP_METHOD_POST,
            ACT_PREFETCH_OBJECTS,
            self.expected_range_template_value,
        )

    def _copy_test_helper(self, starting_val, obj_group):
        to_bck = "to-bucket"
        expected_val = starting_val
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
        expected_val["tobck"] = BucketModel(name=to_bck, provider=to_provider).as_dict()
        expected_val["coer"] = True
        self.object_group_test_helper(
            obj_group.copy,
            HTTP_METHOD_POST,
            ACT_COPY_OBJECTS,
            expected_val,
            to_bck=to_bck,
            to_provider=to_provider,
            continue_on_error=True,
        )

    def test_copy_names(self):
        self._copy_test_helper(
            self.expected_name_list_value, self.object_group_name_list
        )

    def test_copy_range(self):
        self._copy_test_helper(self.expected_range_value, self.object_group_range)

    def test_copy_range_template(self):
        self._copy_test_helper(
            self.expected_range_template_value, self.object_group_template
        )

    def _transform_test_helper(self, starting_val, obj_group):
        etl_name = "any active etl"
        to_bck = "to-bucket"
        expected_val = starting_val
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
        to_provider = "any provider"
        expected_val["tobck"] = BucketModel(name=to_bck, provider=to_provider).as_dict()
        expected_val["coer"] = True
        expected_val["request_timeout"] = timeout
        self.object_group_test_helper(
            obj_group.transform,
            HTTP_METHOD_POST,
            ACT_TRANSFORM_OBJECTS,
            expected_val,
            to_bck=to_bck,
            to_provider=to_provider,
            etl_name=etl_name,
            timeout=timeout,
            continue_on_error=True,
        )

    def test_transform_names(self):
        self._transform_test_helper(
            self.expected_name_list_value, self.object_group_name_list
        )

    def test_transform_range(self):
        self._transform_test_helper(self.expected_range_value, self.object_group_range)

    def test_transform_range_template(self):
        self._transform_test_helper(
            self.expected_range_template_value, self.object_group_template
        )

    def object_group_test_helper(
        self, object_group_function, http_method, action, expected_value, **kwargs
    ):
        resp_text = object_group_function(**kwargs)
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            http_method, action, value=expected_value
        )
