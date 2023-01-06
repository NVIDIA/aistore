import unittest
from unittest.mock import Mock

from aistore.sdk.const import (
    HTTP_METHOD_DELETE,
    ACT_DELETE_MULTIPLE_OBJ,
    ACT_EVICT_MULTIPLE_OBJ,
    HTTP_METHOD_POST,
    ACT_PREFETCH_MULTIPLE_OBJ,
)
from aistore.sdk.object_group import ObjectGroup
from aistore.sdk.object_range import ObjectRange


# pylint: disable=unused-variable
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
        self.obj_names = ["obj-1", "obj-2"]
        self.object_group_list = ObjectGroup(self.mock_bck, obj_names=self.obj_names)
        self.object_group_range = ObjectGroup(self.mock_bck, obj_range=self.obj_range)
        self.expected_list_value = {"objnames": self.obj_names}
        self.expected_range_value = {"template": self.obj_range.string_template()}

    def test_object_group_both_types(self):
        with self.assertRaises(ValueError):
            ObjectGroup(
                self.mock_bck, obj_names=self.obj_names, obj_range=self.obj_range
            )

    def test_delete_list(self):
        resp_text = self.object_group_list.delete()
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            HTTP_METHOD_DELETE, ACT_DELETE_MULTIPLE_OBJ, value=self.expected_list_value
        )

    def test_delete_range(self):
        resp_text = self.object_group_range.delete()
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            HTTP_METHOD_DELETE, ACT_DELETE_MULTIPLE_OBJ, value=self.expected_range_value
        )

    def test_evict_list(self):
        resp_text = self.object_group_list.evict()
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            HTTP_METHOD_DELETE, ACT_EVICT_MULTIPLE_OBJ, value=self.expected_list_value
        )

    def test_evict_range(self):
        resp_text = self.object_group_range.evict()
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            HTTP_METHOD_DELETE, ACT_EVICT_MULTIPLE_OBJ, value=self.expected_range_value
        )

    def test_prefetch_list(self):
        resp_text = self.object_group_list.prefetch()
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            HTTP_METHOD_POST, ACT_PREFETCH_MULTIPLE_OBJ, value=self.expected_list_value
        )

    def test_prefetch_range(self):
        resp_text = self.object_group_range.prefetch()
        self.assertEqual(self.mock_response_text, resp_text)
        self.mock_bck.make_request.assert_called_with(
            HTTP_METHOD_POST, ACT_PREFETCH_MULTIPLE_OBJ, value=self.expected_range_value
        )
