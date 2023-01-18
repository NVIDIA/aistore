#
# Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
#
"""
Allow use of Amazon's boto3 and botocore libraries with AIStore.

If you would like to interact with AIStore using the popular
boto3 (or botocore) libraries, you can do so by importing this
module (you'll also need to install wrapt):

```
import wrapt
from aistore.botocore_patch import botocore
```

By way of explanation: by default, botocore (used by boto3)
does not respect HTTP redirects that make use of the
`location` header. Since AIStore uses them for load
balancing, trying to use boto without patching it first
results in various ClientErrors.

Importing this module patches the botocore library to respect
the location header under the same conditions used by AIStore:
the response code is 301, 302 or 307, and the "location" header
is set.
"""

import inspect
import wrapt


# pylint: disable=unused-argument
def _ais_redirect_wrapper(wrapped, instance, args, kwargs):
    """
    Intercept S3 redirections from AIStore and patch them to:

      - Respect HTTP 301, 302 or 307 redirects using the 'Location' header
      - Fail after 3 redirects for a given operation (rather than just one)

    We make no attempt to differentiate semantically between different 3XX
    codes; we just reuse the same method and request context, and hit the
    url specified by the "location" response header.

    We also don't try to handle caching via 'Cache-Control' or 'Expires'
    (though we might, later).
    """
    response = kwargs.get("response")
    request_dict = kwargs.get("request_dict")

    intercept_criteria = [
        lambda: response is not None,
        lambda: isinstance(response, tuple) and isinstance(request_dict, dict),
        lambda: response[0].status_code in [301, 302, 307],
        lambda: request_dict["context"]["s3_redirect"].get("ais_redirect_count", 0)
        <= 3,
        lambda: {k.lower(): v for k, v in response[0].headers.items()}.get("location"),
    ]

    try:
        if not all(i() for i in intercept_criteria):
            return wrapped(*args, **kwargs)
    except Exception:  # pylint: disable=broad-except
        return wrapped(*args, **kwargs)

    if request_dict["context"]["s3_redirect"].get("ais_redirect_count") is None:
        request_dict["context"]["s3_redirect"]["ais_redirect_count"] = 0
    request_dict["context"]["s3_redirect"]["ais_redirect_count"] += 1

    request_dict["url"] = response[0].headers["location"]

    return 0


@wrapt.when_imported("botocore.utils")
def _apply_patches(module):  # pylint: disable=unused-variable
    """
    When botocore.utils is imported, patch it to handle redirects.

    We can't future proof against everything, but we make a reasonable
    effort to find all likely "S3RegionRedirector" type classes below
    (at time of writing there are two, "S3RegionRedirector" and "S3RegionRedirectorv2".

    If the S3RegionRedirector.redirect_on_error method bound to each
    fails to match our signature, we'll bail.
    """
    redirectors = [
        c[1]
        for c in inspect.getmembers(module, inspect.isclass)
        if "S3RegionRedirector" in c[0]
    ]

    def _expected_sig(self, request_dict, response, operation, **kwargs):
        pass

    for redirector in redirectors:
        assert inspect.signature(redirector.redirect_from_error) == inspect.signature(
            _expected_sig
        )
        wrapt.wrap_function_wrapper(
            module, redirector.__name__ + ".redirect_from_error", _ais_redirect_wrapper
        )
