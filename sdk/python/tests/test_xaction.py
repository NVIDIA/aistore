#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# Default provider is AIS, so all Cloud-related tests are skipped.

import random
import string
import unittest
from aistore.client.errors import ErrBckNotFound

from aistore import Client
from . import CLUSTER_ENDPOINT


class TestObjectOps(unittest.TestCase):  # pylint: disable=unused-variable
    def setUp(self) -> None:
        letters = string.ascii_lowercase
        self.bck_name = "".join(random.choice(letters) for _ in range(10))

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
