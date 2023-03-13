import unittest

from aistore.sdk.multiobj import ObjectNames


# pylint: disable=unused-variable
class TestObjectNames(unittest.TestCase):
    def setUp(self):
        self.name_list = ["obj-1", "obj-2"]
        self.obj_names = ObjectNames(self.name_list)

    def test_get_value(self):
        self.assertEqual({"objnames": self.name_list}, self.obj_names.get_value())

    def test_iter(self):
        self.assertEqual(self.name_list, list(self.obj_names))
