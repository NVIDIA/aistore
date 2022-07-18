#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# Default provider is AIS, so all Cloud-related tests are skipped.

import unittest
from aistore.client.errors import ErrBckNotFound

from aistore import Client
from tests.utils import random_name
from . import CLUSTER_ENDPOINT


class TestObjectOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        self.bck_name = random_name()

        self.client = Client(CLUSTER_ENDPOINT)

    def tearDown(self) -> None:
        # Try to destroy bucket if there is one left.
        try:
            self.client.bucket(self.bck_name).delete()
        except ErrBckNotFound:
            pass

    def test_xaction_start(self):
        self.client.bucket(self.bck_name).create()
        xact_id = self.client.xaction().xact_start(xact_kind="lru")
        self.client.xaction().wait_for_xaction_finished(xact_id=xact_id)


if __name__ == "__main__":
    unittest.main()
