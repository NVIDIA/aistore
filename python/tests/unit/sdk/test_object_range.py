import unittest

from aistore.sdk.errors import InvalidObjectRangeIndex
from aistore.sdk.object_range import ObjectRange
from tests.unit.sdk.test_utils import test_cases


# pylint: disable=unused-variable
class TestObjectRange(unittest.TestCase):
    def setUp(self):
        self.prefix = "prefix-"
        self.suffix = "-suffix"
        self.min_index = 4
        self.max_index = 9
        self.pad_width = 3

    def test_object_range_defaults(self):
        object_range = ObjectRange(
            prefix=self.prefix, min_index=self.min_index, max_index=self.max_index
        )
        self.assertEqual("prefix-{4..9..1}", str(object_range))

    def test_object_range(self):
        object_range = ObjectRange(
            prefix=self.prefix,
            min_index=self.min_index,
            max_index=self.max_index,
            pad_width=self.pad_width,
            step=2,
            suffix=self.suffix,
        )
        self.assertEqual("prefix-{004..009..2}-suffix", str(object_range))

    @test_cases(
        (1, 25, 0, True),
        (25, 1, 0, False),
        (20, 25, 1, False),
        (20, 25, 2, True),
        (20, 25, 3, True),
    )
    def test_validate_indices(self, test_case):
        min_index, max_index, pad_width, valid = test_case
        if valid:
            ObjectRange(
                prefix=self.prefix,
                min_index=min_index,
                max_index=max_index,
                pad_width=pad_width,
            )
            return
        with self.assertRaises(InvalidObjectRangeIndex):
            ObjectRange(
                prefix=self.prefix,
                min_index=min_index,
                max_index=max_index,
                pad_width=pad_width,
            )
