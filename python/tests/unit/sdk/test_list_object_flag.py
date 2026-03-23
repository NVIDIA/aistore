import unittest

from aistore.sdk import ListObjectFlag
from tests.utils import cases


# pylint: disable=unused-variable
class TestListObjectFlag(unittest.TestCase):
    @cases(
        ([], 0),
        ([ListObjectFlag.MISSING], 2),
        ([ListObjectFlag.ONLY_REMOTE_PROPS], 1024),
        ([ListObjectFlag.DELETED, ListObjectFlag.NAME_ONLY], 20),
        ([ListObjectFlag.NO_RECURSION], 2048),
        ([ListObjectFlag.NBI], 32768),
    )
    def test_join_flags(self, test_case):
        self.assertEqual(test_case[1], ListObjectFlag.join_flags(test_case[0]))
