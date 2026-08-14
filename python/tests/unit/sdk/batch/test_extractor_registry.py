#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest

from aistore.sdk.batch.extractor.extractor_registry import get_extractor, _FORMAT_MAP
from aistore.sdk.batch.extractor.tar_stream_extractor import TarStreamExtractor
from aistore.sdk.batch.extractor.zip_stream_extractor import ZipStreamExtractor

from tests.utils import cases


class TestGetExtractor(unittest.TestCase):
    """Unit tests for get_extractor()."""

    @cases(
        # Valid formats (lower/upper/mixed case)
        (".zip", ZipStreamExtractor, None, None),
        (".ZIP", ZipStreamExtractor, None, None),
        (".tar", TarStreamExtractor, None, None),
        (".TAR", TarStreamExtractor, None, None),
        (".Tar", TarStreamExtractor, None, None),
        (".tar.gz", TarStreamExtractor, None, None),
        (".TAR.GZ", TarStreamExtractor, None, None),
        (".tgz", TarStreamExtractor, None, None),
        (".TgZ", TarStreamExtractor, None, None),
        # Unsupported formats
        (".rar", None, ValueError, "Unsupported output format type .rar"),
        (".7z", None, ValueError, "Unsupported output format type .7z"),
        (".bz2", None, ValueError, "Unsupported output format type .bz2"),
        (".xz", None, ValueError, "Unsupported output format type .xz"),
        ("invalid", None, ValueError, "Unsupported output format type invalid"),
        (None, None, ValueError, "Unsupported output format type None"),
        ("", None, ValueError, "Unsupported output format type "),
    )
    def test_get_extractor_cases(self, test_case):
        fmt, expected_type, expect_error, expected_msg = test_case

        if expect_error is not None:
            with self.assertRaises(expect_error) as context:
                get_extractor(fmt)
            self.assertIn(expected_msg, str(context.exception))
        else:
            extractor = get_extractor(fmt)
            self.assertIsInstance(extractor, expected_type)
            # Case-insensitive calls must return the same instance
            if isinstance(fmt, str):
                self.assertIs(extractor, get_extractor(fmt.lower()))

    def test_extractor_reuse(self):
        """Same extractor instance is returned for the same format."""
        self.assertIs(get_extractor(".tar"), get_extractor(".tar"))
        self.assertIs(get_extractor(".tar"), get_extractor(".tar.gz"))
        self.assertIs(get_extractor(".zip"), get_extractor(".zip"))
        self.assertIsNot(get_extractor(".tar"), get_extractor(".zip"))

    def test_format_map_completeness(self):
        """_FORMAT_MAP covers all expected formats with the correct extractor types."""
        expected_tar_formats = {".tar", ".tar.gz", ".tgz"}
        expected_zip_formats = {".zip"}

        self.assertEqual(
            set(_FORMAT_MAP.keys()), expected_tar_formats | expected_zip_formats
        )

        for fmt in expected_tar_formats:
            self.assertIsInstance(_FORMAT_MAP[fmt], TarStreamExtractor)
        for fmt in expected_zip_formats:
            self.assertIsInstance(_FORMAT_MAP[fmt], ZipStreamExtractor)
