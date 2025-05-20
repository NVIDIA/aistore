#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
from tests.botocore_common import BotocoreBaseTest


# pylint: disable=unused-variable,duplicate-code
class PatchedTestCase(BotocoreBaseTest):
    """
    A passthrough test to check we're not breaking botocore
    simply by being imported.

    When botocore is patched, and S3 issues no redirects,
    we should not see any client errors.
    """

    __test__ = True

    def __init__(self, *args, **kwargs):
        from aistore.botocore_patch import botocore

        super().__init__(*args, **kwargs)
        self.enable_redirects = False
        self.redirect_errors_expected = False
