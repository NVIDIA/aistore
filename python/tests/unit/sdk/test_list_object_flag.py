import unittest

from aistore.sdk import ListObjectFlag
from tests.utils import cases


# pylint: disable=unused-variable
class TestListObjectFlag(unittest.TestCase):
    @cases(
        ([], 0),
        ([ListObjectFlag.ALL], 2),
        ([ListObjectFlag.ONLY_REMOTE_PROPS], 1024),
        ([ListObjectFlag.DELETED, ListObjectFlag.NAME_ONLY], 20),
    )
    def test_join_flags(self, test_case):
        self.assertEqual(test_case[1], ListObjectFlag.join_flags(test_case[0]))
