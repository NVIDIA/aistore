#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

import unittest

from aistore.sdk.xact_const import (
    IDLE_KINDS,
    KNOWN_KINDS,
    idles_before_finishing,
    is_valid_kind,
)

# Spell out the expected strings here instead of importing the xact_const
# constants -- otherwise we'd compare the module to itself and miss a changed
# value or an added/removed kind.

# Entries with `Idles: true` in `xact/api_table.go`.
EXPECTED_IDLE_KINDS = frozenset(
    {
        "ec-get",
        "ec-put",
        "ec-resp",
        "put-copies",
        "archive",
        "copy-listrange",
        "etl-listrange",
        "download",
        "list",
        "get-batch",
    }
)

# Full set of kinds present in `xact.Table`.
EXPECTED_KNOWN_KINDS = frozenset(
    {
        "rebalance",
        "resilver",
        "election",
        "lru",
        "cleanup-store",
        "summary-bck",
        "rechunk",
        "index-shard",
        "ec-get",
        "ec-put",
        "ec-resp",
        "ec-encode",
        "put-copies",
        "make-n-copies",
        "archive",
        "copy-listrange",
        "etl-listrange",
        "delete-listrange",
        "evict-listrange",
        "prefetch-listrange",
        "blob-download",
        "download",
        "dsort",
        "promote",
        "move-bck",
        "copy-bck",
        "etl-bck",
        "etl-inline",
        "evict-remote-bck",
        "list",
        "get-batch",
        "create-inventory",
        "load-lom-cache",
    }
)

# Known kinds that must NOT be classified as idle.
EXPECTED_NON_IDLE_KINDS = EXPECTED_KNOWN_KINDS - EXPECTED_IDLE_KINDS


# pylint: disable=unused-variable
class TestXactConst(unittest.TestCase):
    def test_idle_kinds_membership(self):
        for kind in EXPECTED_IDLE_KINDS:
            with self.subTest(kind=kind):
                self.assertTrue(idles_before_finishing(kind))

    def test_non_idle_kinds(self):
        for kind in EXPECTED_NON_IDLE_KINDS:
            with self.subTest(kind=kind):
                self.assertFalse(idles_before_finishing(kind))

    def test_unknown_kind_is_not_idle(self):
        self.assertFalse(idles_before_finishing("nonexistent-xaction"))
        self.assertFalse(idles_before_finishing(""))

    def test_is_valid_kind(self):
        for kind in EXPECTED_KNOWN_KINDS:
            with self.subTest(kind=kind):
                self.assertTrue(is_valid_kind(kind))
        self.assertFalse(is_valid_kind("nonexistent-xaction"))
        self.assertFalse(is_valid_kind(""))

    def test_idle_set_matches_go_table(self):
        self.assertEqual(EXPECTED_IDLE_KINDS, IDLE_KINDS)

    def test_known_set_matches_go_table(self):
        self.assertEqual(EXPECTED_KNOWN_KINDS, KNOWN_KINDS)

    def test_idle_kinds_are_subset_of_known(self):
        self.assertTrue(IDLE_KINDS.issubset(KNOWN_KINDS))


if __name__ == "__main__":
    unittest.main()
