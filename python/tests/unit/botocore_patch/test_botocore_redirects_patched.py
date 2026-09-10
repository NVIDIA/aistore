#
# Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring,import-outside-toplevel,unused-import
import types

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

    # pylint: disable=protected-access,too-few-public-methods
    def test_unexpected_redirector_signature_raises(self):
        """
        Refuse to patch a botocore whose redirector signature we don't expect.

        Must be RuntimeError, not AssertionError
        """
        from aistore.botocore_patch import botocore

        module = types.ModuleType("botocore.utils")

        class S3RegionRedirectorBadSig:
            # missing `operation` and **kwargs
            def redirect_from_error(self, request_dict, response):
                pass

        module.S3RegionRedirectorBadSig = S3RegionRedirectorBadSig

        with self.assertRaises(RuntimeError):
            botocore._apply_patches(module)
