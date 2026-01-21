import unittest

from requests.structures import CaseInsensitiveDict
from aistore.sdk.obj.object_attributes import ObjectAttributes, ObjectAttributesV2


class TestObjectAttributes(unittest.TestCase):
    def setUp(self):
        self.md_dict = {"key1": "value1", "key2": "value2"}
        self.md_string = "key1=value1,key2=value2"
        self.response_headers = CaseInsensitiveDict(
            {
                "Content-Length": "1024",
                "Ais-Checksum-Type": "md5",
                "Ais-Checksum-Value": "abcdef1234567890",
                "ais-atime": "2024-08-13T10:30:00Z",
                "Ais-Version": "1.0",
                "Ais-Custom-Md": self.md_string + ",invalid entry",
            }
        )
        self.attributes = ObjectAttributes(self.response_headers)

    def test_size(self):
        self.assertEqual(1024, self.attributes.size)

    def test_checksum_type(self):
        self.assertEqual("md5", self.attributes.checksum_type)

    def test_checksum_value(self):
        self.assertEqual("abcdef1234567890", self.attributes.checksum_value)

    def test_access_time(self):
        self.assertEqual("2024-08-13T10:30:00Z", self.attributes.access_time)

    def test_obj_version(self):
        self.assertEqual("1.0", self.attributes.obj_version)

    def test_custom_metadata(self):
        self.assertDictEqual(self.md_dict, self.attributes.custom_metadata)

    def test_missing_headers(self):
        headers = CaseInsensitiveDict()
        attributes = ObjectAttributes(headers)
        self.assertEqual(0, attributes.size)
        self.assertEqual("", attributes.checksum_type)
        self.assertEqual("", attributes.checksum_value)
        self.assertEqual("", attributes.access_time)
        self.assertEqual("", attributes.obj_version)
        self.assertDictEqual({}, attributes.custom_metadata)


class TestObjectAttributesV2(unittest.TestCase):
    """Unit tests for ObjectAttributesV2 V2-specific fields."""

    def test_v2_fields(self):
        """Test V2-specific fields: last_modified, etag, chunks."""
        headers = CaseInsensitiveDict(
            {
                "Content-Length": "1024",
                "Last-Modified": "Thu, 15 Jan 2025 10:30:00 GMT",
                "ETag": '"abc123"',
                "ais-chunks-count": "256",
                "ais-chunks-max-chunk-size": "8388608",
            }
        )
        attrs = ObjectAttributesV2(headers)

        # Inherits base
        self.assertEqual(1024, attrs.size)
        # V2 fields
        self.assertEqual("Thu, 15 Jan 2025 10:30:00 GMT", attrs.last_modified)
        self.assertEqual("abc123", attrs.etag)  # quotes stripped
        self.assertIsNotNone(attrs.chunks)
        self.assertEqual(256, attrs.chunks.chunk_count)
        self.assertEqual(8388608, attrs.chunks.max_chunk_size)

    def test_chunks_none_for_monolithic(self):
        """Monolithic objects return None for chunks."""
        headers = CaseInsensitiveDict({"Content-Length": "1024"})
        attrs = ObjectAttributesV2(headers)
        self.assertIsNone(attrs.chunks)
