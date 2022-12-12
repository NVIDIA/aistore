#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#
# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
from tests.botocore_common import BotocoreBaseTest
from tests.integration import CLUSTER_ENDPOINT


# pylint: disable=unused-variable
class IntegrationTestCase(BotocoreBaseTest):
    """
    Run botocore against a real aistore, with
    our patch in place, expecting it to handle
    redirects for us without ClientErrors.
    """

    __test__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        from aistore.botocore_patch import botocore

        self.use_moto = False
        self.endpoint_url = CLUSTER_ENDPOINT + "/s3"
        self.redirect_errors_expected = False
