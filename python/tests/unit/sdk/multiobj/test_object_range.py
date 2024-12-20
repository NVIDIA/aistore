import unittest

from aistore.sdk.errors import InvalidObjectRangeIndex
from aistore.sdk.multiobj import ObjectRange
from tests.utils import cases
from tests.const import PREFIX_NAME, SUFFIX_NAME


# pylint: disable=unused-variable
class TestObjectRange(unittest.TestCase):
    def setUp(self):
        self.prefix = PREFIX_NAME
        self.suffix = SUFFIX_NAME
        self.min_index = 4
        self.max_index = 9
        self.pad_width = 3
        self.step = 2

    def test_object_range_defaults(self):
        object_range = ObjectRange(
            prefix=self.prefix, min_index=self.min_index, max_index=self.max_index
        )
        self.assertEqual(f"{self.prefix}{{4..9..1}}", str(object_range))

    def test_object_range(self):
        object_range = ObjectRange(
            prefix=self.prefix,
            min_index=self.min_index,
            max_index=self.max_index,
            pad_width=self.pad_width,
            step=self.step,
            suffix=self.suffix,
        )
        self.assertEqual(
            f"{self.prefix}{{004..009..2}}{self.suffix}", str(object_range)
        )

    def test_object_range_prefix_only(self):
        object_range = ObjectRange(prefix=self.prefix)
        self.assertEqual(PREFIX_NAME, str(object_range))

    def test_object_range_invalid_suffix(self):
        with self.assertRaises(ValueError):
            ObjectRange(prefix=self.prefix, suffix="anything")

    @cases(
        (1, 25, 0, True),
        (25, 1, 0, False),
        (20, 25, 1, False),
        (None, 25, 1, False),
        (0, None, 1, False),
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

    def test_iter(self):
        object_range = ObjectRange(
            prefix=self.prefix,
            min_index=self.min_index,
            max_index=self.max_index,
            pad_width=self.pad_width,
            step=self.step,
            suffix=self.suffix,
        )
        expected_range = [
            f"{self.prefix}004{self.suffix}",
            f"{self.prefix}006{self.suffix}",
            f"{self.prefix}008{self.suffix}",
        ]
        self.assertEqual(expected_range, list(object_range))
