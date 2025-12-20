import unittest
from unittest.mock import patch, Mock
import requests

from aistore.sdk.obj.content_iterator import (
    ContentIterProvider,
    ParallelContentIterProvider,
)
from aistore.sdk.obj.obj_file.object_file import ObjectFileReader
from aistore.sdk.obj.object_reader import ObjectReader
from aistore.sdk.obj.object_attributes import ObjectAttributes


class TestObjectReader(unittest.TestCase):
    def setUp(self):
        self.object_client = Mock()
        self.chunk_size = 1024
        self.object_reader = ObjectReader(self.object_client, self.chunk_size)
        self.response_headers = {"attr1": "resp1", "attr2": "resp2"}

    def test_head(self):
        mock_attr = Mock()
        self.object_client.head.return_value = mock_attr

        res = self.object_reader.head()

        # Attributes should be returned and the property updated
        self.assertEqual(res, mock_attr)
        self.assertEqual(mock_attr, self.object_reader.attributes)
        self.object_client.head.assert_called_once()

    def test_attributes_property(self):
        mock_attr = Mock()
        self.object_client.head.return_value = mock_attr

        attr = self.object_reader.attributes

        # Attributes should be returned and the property updated
        self.assertEqual(attr, mock_attr)
        # If we access attributes again, no new call to the client
        attr = self.object_reader.attributes
        self.assertEqual(attr, mock_attr)
        self.object_client.head.assert_called_once()

    @patch("aistore.sdk.obj.object_reader.ObjectAttributes", autospec=True)
    def test_read_all(self, mock_attr):
        # Should return the response content and update the attributes
        chunk1 = b"chunk1"
        chunk2 = b"chunk2"
        mock_response = Mock(
            spec=requests.Response,
            content=chunk1 + chunk2,
            headers=self.response_headers,
        )
        self.object_client.get.return_value = mock_response

        content = self.object_reader.read_all()

        # Assert the result, the call to object client
        self.assertEqual(chunk1 + chunk2, content)
        self.object_client.get.assert_called_with(stream=False)
        # Assert attributes parsed and updated
        self.assertIsInstance(self.object_reader.attributes, ObjectAttributes)
        mock_attr.assert_called_with(self.response_headers)

    @patch("aistore.sdk.obj.object_reader.ObjectAttributes", autospec=True)
    def test_raw(self, mock_attr):
        mock_response = Mock(
            spec=requests.Response, raw=b"bytestream", headers=self.response_headers
        )
        self.object_client.get.return_value = mock_response

        raw_stream = self.object_reader.raw()

        # Assert the result, the call to object client
        self.assertEqual(mock_response.raw, raw_stream)
        self.object_client.get.assert_called_with(stream=True)
        # Assert attributes parsed and updated
        self.assertIsInstance(self.object_reader.attributes, ObjectAttributes)
        mock_attr.assert_called_with(self.response_headers)

    @patch("aistore.sdk.obj.object_reader.ContentIterProvider")
    def test_iter(self, mock_provider_cls):
        mock_provider = Mock()
        iterable_bytes = iter(b"test")
        mock_provider.create_iter.return_value = iterable_bytes
        mock_provider_cls.return_value = mock_provider

        # Re-create to use the patched provider
        self.object_reader = ObjectReader(self.object_client)
        res = iter(self.object_reader)

        mock_provider.create_iter.assert_called_with()
        self.assertEqual(iterable_bytes, res)

    @patch("aistore.sdk.obj.object_reader.ObjectFileReader", autospec=True)
    def test_as_file(self, mock_obj_file):
        # Returns an ObjectFileReader with the default resume count
        res = self.object_reader.as_file()
        self.assertIsInstance(res, ObjectFileReader)
        mock_obj_file.assert_called_once()
        # Get the arguments passed to the mock
        args, kwargs = mock_obj_file.call_args
        # For now just check that we provided a content iterator
        self.assertIsInstance(args[0], ContentIterProvider)
        # Check the max_resume argument
        self.assertEqual(kwargs.get("max_resume"), 5)

    @patch("aistore.sdk.obj.object_reader.ObjectFileReader", autospec=True)
    def test_as_file_max_resume(self, mock_obj_file):
        max_resume = 12
        # Returns an ObjectFileReader with the default resume count
        res = self.object_reader.as_file(max_resume=max_resume)
        self.assertIsInstance(res, ObjectFileReader)
        mock_obj_file.assert_called_once()
        # Get the arguments passed to the mock
        args, kwargs = mock_obj_file.call_args
        # For now just check that we provided a content iterator
        self.assertIsInstance(args[0], ContentIterProvider)
        # Check the max_resume argument
        self.assertEqual(kwargs.get("max_resume"), max_resume)

    def test_as_file_invalid_max_resume(self):
        # Test for invalid max_resume value
        with self.assertRaises(ValueError) as context:
            self.object_reader.as_file(max_resume=-1)
        self.assertEqual(
            str(context.exception),
            "Invalid max_resume (must be a non-negative integer): -1.",
        )

    def test_num_workers_uses_parallel_provider(self):
        """Test that providing num_workers uses ParallelContentIterProvider."""
        mock_attrs = Mock()
        mock_attrs.size = 1000
        self.object_client.head.return_value = mock_attrs
        self.object_client.get_range.return_value = b"data"

        reader = ObjectReader(self.object_client, self.chunk_size, num_workers=4)

        # pylint: disable=protected-access
        self.assertIsInstance(reader._content_provider, ParallelContentIterProvider)

    def test_no_num_workers_uses_sequential_provider(self):
        """Test that not providing num_workers uses ContentIterProvider."""
        reader = ObjectReader(self.object_client, self.chunk_size)

        # pylint: disable=protected-access
        self.assertIsInstance(reader._content_provider, ContentIterProvider)

    def test_num_workers_calls_head(self):
        """Test that num_workers triggers a HEAD request for object size."""
        mock_attrs = Mock()
        mock_attrs.size = 1000
        self.object_client.head.return_value = mock_attrs

        ObjectReader(self.object_client, self.chunk_size, num_workers=4)

        self.object_client.head.assert_called_once()
