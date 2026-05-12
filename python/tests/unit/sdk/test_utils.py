#
# Copyright (c) 2022-2025, NVIDIA CORPORATION. All rights reserved.
#
import unittest
from unittest.mock import Mock, patch, mock_open
from urllib.parse import urlencode

import requests
from msgspec import msgpack
from requests import Response, PreparedRequest

from aistore.sdk.const import (
    MSGPACK_CONTENT_TYPE,
    HEADER_CONTENT_TYPE,
    XX_HASH_SEED,
    QPARAM_NAMESPACE,
    QPARAM_PROVIDER,
)
from aistore.sdk.provider import Provider

from aistore.sdk.utils import (
    decode_response,
    expand_braces,
    get_file_size,
    get_object_url_components,
    probing_frequency,
    read_file_bytes,
    validate_directory,
    validate_file,
    xoshiro256_hash,
    get_digest,
    get_provider_from_request,
    extract_and_parse_url,
)
from tests.const import PREFIX_NAME
from tests.utils import cases


# pylint: disable=unused-variable
class TestUtils(unittest.TestCase):
    @cases((0, 0.1), (-1, 0.1), (64, 1), (128, 2), (100000, 1562.5))
    def test_probing_frequency(self, test_case):
        self.assertEqual(test_case[1], probing_frequency(test_case[0]))

    @patch("pathlib.Path.is_file")
    @patch("pathlib.Path.exists")
    def test_validate_file(self, mock_exists, mock_is_file):
        mock_exists.return_value = False
        with self.assertRaises(ValueError):
            validate_file("any path")
        mock_exists.return_value = True
        mock_is_file.return_value = False
        with self.assertRaises(ValueError):
            validate_file("any path")
        mock_is_file.return_value = True
        validate_file("any path")

    @patch("pathlib.Path.is_dir")
    @patch("pathlib.Path.exists")
    def test_validate_dir(self, mock_exists, mock_is_dir):
        mock_exists.return_value = False
        with self.assertRaises(ValueError):
            validate_directory("any path")
        mock_exists.return_value = True
        mock_is_dir.return_value = False
        with self.assertRaises(ValueError):
            validate_directory("any path")
        mock_is_dir.return_value = True
        validate_directory("any path")

    def test_read_file_bytes(self):
        data = b"Test data"
        with patch("builtins.open", mock_open(read_data=data)):
            res = read_file_bytes("any path")
        self.assertEqual(data, res)

    @cases((123, "123 Bytes"), (None, "unknown"))
    def test_get_file_size(self, test_case):
        mock_file = Mock()
        mock_file.stat.return_value = Mock(st_size=test_case[0])
        self.assertEqual(test_case[1], get_file_size(mock_file))

    @cases(
        (PREFIX_NAME, [PREFIX_NAME], None),
        ("prefix-{}", ["prefix-{}"], None),
        ("prefix-{0..1..2..3}", ["prefix-{0..1..2..3}"], None),
        ("prefix-{0..1..2}}", [], ValueError),
        (
            "prefix-{1..6..2}-gap-{12..14..1}-suffix",
            [
                "prefix-1-gap-12-suffix",
                "prefix-1-gap-13-suffix",
                "prefix-1-gap-14-suffix",
                "prefix-3-gap-12-suffix",
                "prefix-3-gap-13-suffix",
                "prefix-3-gap-14-suffix",
                "prefix-5-gap-12-suffix",
                "prefix-5-gap-13-suffix",
                "prefix-5-gap-14-suffix",
            ],
            None,
        ),
    )
    def test_expand_braces(self, test_case):
        input_str, output, expected_error = test_case
        if not expected_error:
            self.assertEqual(output, list(expand_braces(input_str)))
        else:
            with self.assertRaises(expected_error):
                expand_braces(input_str)

    @patch("aistore.sdk.utils.TypeAdapter")
    def test_decode_response_json(self, mock_adapter):
        response_content = "text content"
        parsed_content = "parsed content"
        mock_response = Mock(Response)
        mock_response.headers = {}
        mock_response.text = response_content

        mock_validator = Mock()
        mock_adapter.return_value = mock_validator
        mock_validator.validate_json.return_value = parsed_content

        res = decode_response(str, mock_response)

        self.assertEqual(parsed_content, res)
        mock_adapter.assert_called_with(str)
        mock_validator.validate_json.assert_called_with(response_content)

    def test_decode_response_msgpack(self):
        unpacked_content = {"content key": "content value"}
        packed_content = msgpack.encode(unpacked_content)
        mock_response = Mock(Response)
        mock_response.headers = {HEADER_CONTENT_TYPE: MSGPACK_CONTENT_TYPE}
        mock_response.content = packed_content

        res = decode_response(dict, mock_response)

        self.assertEqual(unpacked_content, res)

    @cases(
        (123456789, 5288836854215336256),
        (0, 1905207664160064169),
        (2**64 - 1, 10227601306713020730),
    )
    def test_xoshiro256_hash(self, test_case):
        seed, expected_result = test_case
        result = xoshiro256_hash(seed)
        self.assertIsInstance(result, int)
        self.assertGreaterEqual(result, 0)
        self.assertLess(result, 2**64)  # Ensure 64-bit overflow behavior
        self.assertEqual(expected_result, result)

    @patch("aistore.sdk.utils.xxhash.xxh64")
    def test_get_digest(self, mock_xxhash):
        mock_xxhash.return_value.intdigest.return_value = 987654321
        name = "test_object"
        result = get_digest(name)
        mock_xxhash.assert_called_once_with(
            seed=XX_HASH_SEED, input=name.encode("utf-8")
        )
        self.assertEqual(result, 987654321)

    def test_get_provider_from_request(self):
        for provider in list(Provider):
            req = Mock(spec=requests.Request, params={QPARAM_PROVIDER: provider.value})
            self.assertEqual(provider, get_provider_from_request(req))

    def test_get_provider_from_request_prepared(self):
        for provider in list(Provider):
            query = urlencode({QPARAM_PROVIDER: provider.value})
            req = PreparedRequest()
            req.url = f"https://example.com/path?{query}"
            self.assertEqual(provider, get_provider_from_request(req))

    @cases(
        Mock(spec=requests.Request, params={}),
        Mock(spec=requests.PreparedRequest, url="https://example.com/path"),
    )
    def test_get_provider_from_request_invalid(self, req):
        with self.assertRaises(ValueError):
            get_provider_from_request(req)

    @staticmethod
    def _prepared(url: str) -> PreparedRequest:
        req = PreparedRequest()
        req.url = url
        return req

    def test_get_object_url_components_drops_unrequested_qparams(self):
        """Only the qparams the caller asks for survive; CSK signing params,
        feature flags, and any other extras are stripped."""
        req = self._prepared(
            "https://target:8081/v1/objects/mybkt/myobj"
            "?provider=aws&latest=true&pid=p1&hsig=s&hnonce=42&hsmap=7"
        )
        path, params = get_object_url_components(req, [QPARAM_PROVIDER])
        self.assertEqual(path, "objects/mybkt/myobj")
        # Raw qparam value is preserved as-is
        self.assertEqual(params, {QPARAM_PROVIDER: "aws"})

    def test_get_object_url_components_preserves_namespace(self):
        """Namespace qparam (e.g. remote-AIS @uuid#ns) is preserved alongside
        provider when the caller asks for it."""
        req = self._prepared(
            "https://target:8081/v1/objects/mybkt/myobj"
            f"?provider=aws&{QPARAM_NAMESPACE}=%40uuid123%23ns&latest=true"
        )
        path, params = get_object_url_components(
            req, [QPARAM_PROVIDER, QPARAM_NAMESPACE]
        )
        self.assertEqual(path, "objects/mybkt/myobj")
        self.assertEqual(
            params,
            {
                QPARAM_PROVIDER: "aws",
                QPARAM_NAMESPACE: "@uuid123#ns",
            },
        )

    def test_get_object_url_components_skips_missing_requested_qparams(self):
        """Requested qparams that aren't on the URL are silently omitted —
        the caller gets back only what was actually present."""
        req = self._prepared("https://target:8081/v1/objects/mybkt/myobj?provider=aws")
        _, params = get_object_url_components(req, [QPARAM_PROVIDER, QPARAM_NAMESPACE])
        self.assertEqual(params, {QPARAM_PROVIDER: "aws"})

    @cases(
        # Object name contains "objects/" — split must keep the tail intact.
        (
            "https://target:8081/v1/objects/mybkt/path/objects/blob.bin?provider=aws",
            "objects/mybkt/path/objects/blob.bin",
        ),
        # Bucket literally named "objects".
        (
            "https://target:8081/v1/objects/objects/foo?provider=aws",
            "objects/objects/foo",
        ),
        # Bucket "objects" AND object starting with "objects/".
        (
            "https://target:8081/v1/objects/objects/objects/blob?provider=aws",
            "objects/objects/objects/blob",
        ),
    )
    def test_get_object_url_components_marker_robustness(self, test_case):
        url, expected_path = test_case
        path, _ = get_object_url_components(self._prepared(url), [QPARAM_PROVIDER])
        self.assertEqual(path, expected_path)

    def test_get_object_url_components_no_marker_raises(self):
        req = self._prepared("https://target:8081/v1/buckets/mybkt?provider=aws")
        with self.assertRaises(ValueError):
            get_object_url_components(req, [QPARAM_PROVIDER])

    @cases(
        ("bucket 'ais://bucket' does not exist", ("ais", "bucket", False)),
        ("object 'ais://bucket/object.txt' does not exist", ("ais", "bucket", True)),
        ("bucket 'aws://bucket' does not exist", ("aws", "bucket", False)),
        ("bucket 's3://bucket' does not exist", ("s3", "bucket", False)),
        ("bucket 'gcp://bucket' does not exist", ("gcp", "bucket", False)),
        ("object 'gs://bucket/obj' does not exist", ("gs", "bucket", True)),
        ("bucket 'azure://b' does not exist", ("azure", "b", False)),
        ("object 'ht://b/o' does not exist", ("ht", "b", True)),
        ("object 'oci://bucket/obj' does not exist", ("oci", "bucket", True)),
        ("bucket 'abc://bucket' does not exist", None),
        ("object 'abc://bucket/obj' does not exist", None),
        (f"bucket 'ais://{'a'*133}' does not exist", None),
        (f"object 'ais://{'a'*133}/obj' does not exist", None),
    )
    def test_extract_and_parse_url(self, test_case):
        url_or_msg, expected = test_case
        self.assertEqual(expected, extract_and_parse_url(url_or_msg))
