import unittest
from unittest.mock import Mock

from aistore.sdk.object_iterator import ObjectIterator
from aistore.sdk.types import BucketEntry


class TestObjectIterator(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.callable_resp = Mock()
        self.callable_resp.continuation_token = ""
        self.callable = lambda uuid, token: self.callable_resp
        self.obj_iterator = ObjectIterator(self.callable)

    def test_iter(self):
        self.assertEqual(self.obj_iterator, iter(self.obj_iterator))

    def test_next_empty_resp(self):
        with self.assertRaises(StopIteration):
            self.callable_resp.entries = []
            self.callable_resp.uuid = ""
            self.callable_resp.continuation_token = ""
            next(self.obj_iterator)

    def test_next_iterator_exhausted(self):
        entry_1 = Mock(BucketEntry)
        entry_2 = Mock(BucketEntry)
        entry_3 = Mock(BucketEntry)
        self.callable_resp.entries = [entry_1, entry_2, entry_3]
        self.callable_resp.uuid = "UUID"
        self.assertEqual(entry_1, next(self.obj_iterator))
        self.assertEqual(entry_2, next(self.obj_iterator))
        self.assertEqual(entry_3, next(self.obj_iterator))
        with self.assertRaises(StopIteration):
            next(self.obj_iterator)

    def test_next_multiple_pages(self):
        entry_1 = Mock(BucketEntry)
        entry_2 = Mock(BucketEntry)
        entry_3 = Mock(BucketEntry)
        self.callable_resp.entries = [entry_1, entry_2]
        self.callable_resp.uuid = ""
        self.assertEqual(entry_1, next(self.obj_iterator))
        self.assertEqual(entry_2, next(self.obj_iterator))
        self.callable_resp.entries = [entry_3]
        self.assertEqual(entry_3, next(self.obj_iterator))
        with self.assertRaises(StopIteration):
            next(self.obj_iterator)
