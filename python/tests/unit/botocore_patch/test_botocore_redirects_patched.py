#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
from tests.botocore_common import BotocoreBaseTest


# pylint: disable=unused-variable
class PatchedRedirectingTestCase(BotocoreBaseTest):
    """
    This directly tests our monkey patch.

    When botocore is patched, and S3 issues redirects,
    we should not see any client errors.
    """

    __test__ = True

    def __init__(self, *args, **kwargs):
        from aistore.botocore_patch import botocore

        super().__init__(*args, **kwargs)
        self.enable_redirects = True
        self.redirect_errors_expected = False
