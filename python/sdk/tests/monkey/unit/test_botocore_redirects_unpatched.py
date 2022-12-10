#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
from tests.monkey.botocore_common import BotocoreBaseTest


# pylint: disable=unused-variable
class UnpatchedRedirectingTestCase(BotocoreBaseTest):
    """
    Another control case.

    When botocore is unpatched, and S3 issues redirects,
    we should see some client errors.
    """

    __test__ = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enable_redirects = True
        self.redirect_errors_expected = True
