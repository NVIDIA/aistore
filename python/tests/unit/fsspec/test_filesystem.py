#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import Mock

from aistore.fsspec import AISFileSystem
from aistore.sdk.types import BucketEntry


class TestAISFileSystem(unittest.TestCase):
    def setUp(self) -> None:
        self.client = Mock()
        self.bucket = self.client.bucket.return_value
        self.fs = AISFileSystem(endpoint="http://ais.example", client=self.client)

    def test_ls_returns_fsspec_file_info(self):
        self.bucket.list_objects_iter.return_value = [
            BucketEntry(n="data/a.csv", s=11),
            BucketEntry(n="data/b.csv", s=13),
        ]

        result = self.fs.ls("ais://dataset/data")

        self.client.bucket.assert_called_once_with("dataset", provider="ais")
        self.bucket.list_objects_iter.assert_called_once_with(
            prefix="data", props="name,size"
        )
        self.assertEqual(
            [
                {"name": "dataset/data/a.csv", "size": 11, "type": "file"},
                {"name": "dataset/data/b.csv", "size": 13, "type": "file"},
            ],
            result,
        )

    def test_info_returns_object_size(self):
        props = Mock(size=42)
        self.bucket.object.return_value.props = props

        result = self.fs.info("ais://dataset/data/a.csv")

        self.bucket.object.assert_called_once_with("data/a.csv")
        self.assertEqual(
            {"name": "dataset/data/a.csv", "size": 42, "type": "file"}, result
        )

    def test_cat_file_uses_half_open_fsspec_range(self):
        reader = Mock()
        reader.read_all.return_value = b"bc"
        self.bucket.object.return_value.get_reader.return_value = reader

        result = self.fs.cat_file("ais://dataset/data/a.csv", start=1, end=3)

        self.bucket.object.return_value.get_reader.assert_called_once_with(
            byte_range="bytes=1-2"
        )
        self.assertEqual(b"bc", result)

    def test_open_returns_sdk_file_reader(self):
        file_reader = Mock()
        self.bucket.object.return_value.get_reader.return_value.as_file.return_value = file_reader

        result = self.fs.open("ais://dataset/data/a.csv", mode="rb")

        self.assertEqual(file_reader, result)

    def test_write_mode_is_not_supported(self):
        with self.assertRaises(NotImplementedError):
            self.fs.open("ais://dataset/data/a.csv", mode="wb")


if __name__ == "__main__":
    unittest.main()
