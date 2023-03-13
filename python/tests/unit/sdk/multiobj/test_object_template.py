import unittest
from unittest.mock import patch

from aistore.sdk.multiobj import ObjectTemplate


# pylint: disable=unused-variable
class TestObjectTemplate(unittest.TestCase):
    def setUp(self):
        self.template_str = "prefix-{1..6..2}-gap-{12..14..1}-suffix"
        self.obj_range_template = ObjectTemplate(self.template_str)

    def test_get_value(self):
        self.assertEqual(
            {"template": self.template_str}, self.obj_range_template.get_value()
        )

    @patch("aistore.sdk.multiobj.object_template.utils.expand_braces")
    def test_iter(self, mock_expand):
        expansion_result = ["mock expansion result", "result2"]
        mock_expand.return_value.__next__.side_effect = expansion_result
        self.assertEqual(
            expansion_result,
            list(self.obj_range_template),
        )
