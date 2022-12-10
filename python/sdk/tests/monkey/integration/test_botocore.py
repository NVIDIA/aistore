#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
from tests.monkey.botocore_common import BotocoreBaseTest
from . import CLUSTER_ENDPOINT


# pylint: disable=unused-variable
class UnpatchedIntegrationTestCase(BotocoreBaseTest):
    """
    Integration control case.

    When botocore is unpatched, trying to use a real
    live AIStore should throw a bunch of ClientErrors.
    """

    __test__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from aistore.monkey import botocore

        self.use_moto = False
        self.endpoint_url = CLUSTER_ENDPOINT + "/s3"
        self.redirect_errors_expected = False
