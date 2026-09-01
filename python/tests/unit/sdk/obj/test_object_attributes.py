import unittest

from requests.structures import CaseInsensitiveDict

from aistore.sdk.obj.object_attributes import ECInfo, ObjectAttributes


class TestObjectAttributes(unittest.TestCase):
    def setUp(self):
        self.md_dict = {"key1": "value1", "key2": "value2", "key3": "a=b"}
        self.response_headers = CaseInsensitiveDict(
            {
                "Content-Length": "1024",
                "Ais-Checksum-Type": "md5",
                "Ais-Checksum-Value": "abcdef1234567890",
                "Ais-Atime": "2024-08-13T10:30:00Z",
                "Ais-Version": "1.0",
                "Ais-Custom-Md": "key1=value1,key2=value2,key3=a=b,invalid entry",
                "Ais-Present": "true",
            }
        )
        self.attributes = ObjectAttributes(self.response_headers)

    def test_base_attributes(self):
        self.assertEqual(1024, self.attributes.size)
        self.assertEqual("md5", self.attributes.checksum_type)
        self.assertEqual("abcdef1234567890", self.attributes.checksum_value)
        self.assertEqual("2024-08-13T10:30:00Z", self.attributes.access_time)
        self.assertEqual("1.0", self.attributes.obj_version)
        self.assertDictEqual(self.md_dict, self.attributes.custom_metadata)
        self.assertTrue(self.attributes.present)

    def test_selective_attributes(self):
        headers = CaseInsensitiveDict(
            {
                "Ais-Location": "t[node]:mp[/tmp/ais/mp1]",
                "Ais-Mirror-Copies": "2",
                "Ais-Mirror-Paths": "/tmp/ais/mp1,/tmp/ais/mp2",
                "Ais-Ec-Generation": "3",
                "Ais-Ec-Data": "2",
                "Ais-Ec-Parity": "1",
                "Ais-Ec-Replicated": "true",
                "Last-Modified": "Thu, 15 Jan 2025 10:30:00 GMT",
                "ETag": '"abc123"',
                "Ais-Chunks-Count": "256",
                "Ais-Chunks-Max-Chunk-Size": "8388608",
            }
        )
        attributes = ObjectAttributes(headers)

        self.assertEqual("t[node]:mp[/tmp/ais/mp1]", attributes.location)
        self.assertEqual(2, attributes.mirror_copies)
        self.assertEqual(["/tmp/ais/mp1", "/tmp/ais/mp2"], attributes.mirror_paths)
        self.assertEqual(
            ECInfo(generation=3, data_slices=2, parity_slices=1, is_ec_copy=True),
            attributes.ec,
        )
        self.assertEqual("Thu, 15 Jan 2025 10:30:00 GMT", attributes.last_modified)
        self.assertEqual("abc123", attributes.etag)
        self.assertEqual(256, attributes.chunks.chunk_count)
        self.assertEqual(8388608, attributes.chunks.max_chunk_size)

    def test_missing_headers(self):
        attributes = ObjectAttributes(CaseInsensitiveDict())

        self.assertEqual(0, attributes.size)
        self.assertEqual("", attributes.checksum_type)
        self.assertEqual("", attributes.checksum_value)
        self.assertEqual("", attributes.access_time)
        self.assertEqual("", attributes.obj_version)
        self.assertDictEqual({}, attributes.custom_metadata)
        self.assertFalse(attributes.present)
        self.assertEqual("", attributes.location)
        self.assertEqual([], attributes.mirror_paths)
        self.assertEqual(0, attributes.mirror_copies)
        self.assertIsNone(attributes.ec)
        self.assertEqual("", attributes.last_modified)
        self.assertEqual("", attributes.etag)
        self.assertIsNone(attributes.chunks)

    def test_empty_mirror_paths_header(self):
        attributes = ObjectAttributes(CaseInsensitiveDict({"Ais-Mirror-Paths": "[]"}))

        self.assertEqual([], attributes.mirror_paths)

    def test_malformed_numeric_headers(self):
        attributes = ObjectAttributes(
            CaseInsensitiveDict(
                {
                    "Content-Length": "not-a-number",
                    "Ais-Chunks-Count": "0x10",
                    "Ais-Chunks-Max-Chunk-Size": "1e6",
                    "Ais-Mirror-Copies": "two",
                    "Ais-Ec-Data": "many",
                }
            )
        )

        self.assertEqual(0, attributes.size)
        self.assertEqual(0, attributes.chunks.chunk_count)
        self.assertEqual(0, attributes.chunks.max_chunk_size)
        self.assertEqual(0, attributes.mirror_copies)
        self.assertEqual(0, attributes.ec.data_slices)
