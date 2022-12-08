#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
from tests.monkey.botocore_common import BotocoreBaseTest


# pylint: disable=unused-variable
class UnpatchedTestCase(BotocoreBaseTest):
    """
    Our control case.

    When botocore is unpatched, and S3 issues no redirects,
    we should not see any client errors.
    """

    __test__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enable_redirects = False
        self.redirect_errors_expected = False
