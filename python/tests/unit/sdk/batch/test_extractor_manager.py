#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import unittest
from unittest.mock import patch, Mock

from aistore.sdk.batch.extractor.extractor_manager import ExtractorManager
from aistore.sdk.batch.extractor.tar_stream_extractor import TarStreamExtractor
from aistore.sdk.batch.extractor.zip_stream_extractor import ZipStreamExtractor

from tests.utils import cases


# pylint: disable=protected-access
class TestExtractorManager(unittest.TestCase):
    """Unit tests for ExtractorManager class."""

    def setUp(self):
        """Reset singleton instance before each test."""
        ExtractorManager._instance = None

    def tearDown(self):
        """Clean up singleton instance after each test."""
        ExtractorManager._instance = None

    def test_singleton_pattern(self):
        """Test that ExtractorManager follows singleton pattern."""
        # Create two instances
        manager1 = ExtractorManager()
        manager2 = ExtractorManager()

        # Should be the same instance
        self.assertIs(manager1, manager2)
        self.assertEqual(id(manager1), id(manager2))

    @patch("aistore.sdk.batch.extractor.extractor_manager.TarStreamExtractor")
    @patch("aistore.sdk.batch.extractor.extractor_manager.ZipStreamExtractor")
    def test_initialization_creates_format_map(
        self, mock_zip_extractor_class, mock_tar_extractor_class
    ):
        """Test that initialization properly creates format map from extractors."""
        # Setup mock extractors
        mock_tar_extractor = Mock()
        mock_tar_extractor.get_supported_formats.return_value = (
            ".tar",
            ".tar.gz",
            ".tgz",
        )
        mock_tar_extractor_class.return_value = mock_tar_extractor

        mock_zip_extractor = Mock()
        mock_zip_extractor.get_supported_formats.return_value = (".zip",)
        mock_zip_extractor_class.return_value = mock_zip_extractor

        # Create manager
        manager = ExtractorManager()

        # Verify extractors were instantiated
        mock_tar_extractor_class.assert_called_once()
        mock_zip_extractor_class.assert_called_once()

        # Verify get_supported_formats was called on each extractor
        mock_tar_extractor.get_supported_formats.assert_called_once()
        mock_zip_extractor.get_supported_formats.assert_called_once()

        # Verify format map was created correctly
        expected_formats = {
            ".tar": mock_tar_extractor,
            ".tar.gz": mock_tar_extractor,
            ".tgz": mock_tar_extractor,
            ".zip": mock_zip_extractor,
        }
        self.assertEqual(manager._format_map, expected_formats)

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
        manager = ExtractorManager()

        if expect_error is not None:
            with self.assertRaises(expect_error) as context:
                manager.get_extractor(fmt)
            self.assertIn(expected_msg, str(context.exception))
        else:
            extractor = manager.get_extractor(fmt)
            self.assertIsInstance(extractor, expected_type)
            # Also test that case-insensitive calls return the same instance
            if isinstance(fmt, str):
                canonical_lower = fmt.lower()
                extractor_lower = manager.get_extractor(canonical_lower)
                self.assertIs(extractor, extractor_lower)

    def test_extractor_reuse(self):
        """Test that the same extractor instance is reused for same format."""
        manager = ExtractorManager()

        # Get extractor for same format multiple times
        extractor1 = manager.get_extractor(".tar")
        extractor2 = manager.get_extractor(".tar")
        extractor3 = manager.get_extractor(".tar.gz")

        # Should be the same instance for TAR formats
        self.assertIs(extractor1, extractor2)
        self.assertIs(extractor1, extractor3)

        # Get ZIP extractor
        zip_extractor1 = manager.get_extractor(".zip")
        zip_extractor2 = manager.get_extractor(".zip")

        # Should be the same ZIP instance
        self.assertIs(zip_extractor1, zip_extractor2)

        # But TAR and ZIP extractors should be different
        self.assertIsNot(extractor1, zip_extractor1)

    def test_format_map_completeness(self):
        """Test that format map includes all expected formats."""
        manager = ExtractorManager()

        # Expected formats based on current extractors
        expected_tar_formats = {".tar", ".tar.gz", ".tgz"}
        expected_zip_formats = {".zip"}
        all_expected_formats = expected_tar_formats | expected_zip_formats

        # Check that all expected formats are in the map
        actual_formats = set(manager._format_map.keys())
        self.assertEqual(actual_formats, all_expected_formats)

        # Verify TAR formats map to TarStreamExtractor
        for fmt in expected_tar_formats:
            self.assertIsInstance(manager._format_map[fmt], TarStreamExtractor)

        # Verify ZIP formats map to ZipStreamExtractor
        for fmt in expected_zip_formats:
            self.assertIsInstance(manager._format_map[fmt], ZipStreamExtractor)

    @patch("aistore.sdk.batch.extractor.extractor_manager.TarStreamExtractor")
    def test_init_only_called_once_for_singleton(self, mock_tar_extractor_class):
        """Test that __init__ is only called once for singleton."""
        mock_tar_extractor = Mock()
        mock_tar_extractor.get_supported_formats.return_value = (".tar",)
        mock_tar_extractor_class.return_value = mock_tar_extractor

        # Create multiple instances
        manager1 = ExtractorManager()
        manager2 = ExtractorManager()
        manager3 = ExtractorManager()

        # __init__ should only be called once (for the first instance)
        # The extractor class should only be instantiated once
        mock_tar_extractor_class.assert_called_once()

        # All managers should be the same instance
        self.assertIs(manager1, manager2)
        self.assertIs(manager2, manager3)
