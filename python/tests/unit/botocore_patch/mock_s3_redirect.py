#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring
import logging
import wrapt

# Patch moto - an S3 mocking library to simulate HTTP redirects,
# which might happen on AIStore but the moto library don't.
# This enhances moto to emulate AIStore behavior more comprehensively,
# Specifically, if the `redirections_enabled` is set to `True` by user,
# fake HTTP redirects for the following operations.
#
# Not all response operatons found in moto's
# S3Response are found below; they don't all
# provide us with sufficient information to
# patch successfully.
test_ops = [
    "_bucket_response_put",
    "_bucket_response_post",
    "_bucket_response_delete_keys",
    "_key_response_get",
    "_key_response_put",
    "_key_response_delete",
    "_key_response_post",
]

redirected_ops = test_ops
redirections_enabled = True  # pylint: disable=invalid-name


def s3_response_wrapper(
    wrapped, instance, args, kwargs
):  # pylint: disable=unused-argument
    """
    Patch various internal S3Response methods in moto to issue
    redirects.

    args and kwargs are somewhat inconsistent and vary per HTTP method.
    in particular, HEAD doesn't have enough scrapable context for us to
    redirect, so we don't bother.
    """
    url = None
    operation = wrapped.__name__

    should_redirect = operation in redirected_ops and redirections_enabled
    ret = wrapped(*args, **kwargs)
    if not should_redirect:
        return ret

    logging.debug("s3_response_wrapper: intercepted %s", wrapped.__name__)
    method = None
    try:
        method = operation.split("_").pop().upper()
        if method == "GET":
            bucket = args[0]
            url = f"https://s3.amazonaws.com/{bucket}"
            key = args[-1]
            if isinstance(key, str):
                url += f"/{key}"
            attempt = int(
                kwargs["headers"]["amz-sdk-request"].split(";")[0].split("=")[1]
            )
        elif method == "PUT":
            url = args[0].__dict__["url"]
            attempt = int(
                args[0]
                .__dict__["headers"]["amz-sdk-request"]
                .split(";")[0]
                .split("=")[1]
            )
        elif method == "DELETE":
            bucket = args[1]
            url = f"https://s3.amazonaws.com/{bucket}"
            key = args[-1]
            if isinstance(key, str):
                url += f"/{key}"
            attempt = int(args[0]["amz-sdk-request"].split(";")[0].split("=")[1])
        elif method == "HEAD":
            url = None
            attempt = None
        elif method == "POST":
            url = None
            attempt = None
        logging.debug(
            "s3_response_wrapper: parsed operation %s, method %s, url %s, attempt %s",
            operation,
            method,
            url,
            attempt,
        )
    except Exception:  # pylint: disable=broad-except
        pass

    logging.debug("s3_response_wrapper: redirecting operation %s", operation)
    if attempt < 5:
        ret = list(ret)
        ret[0] = 307
        ret[1]["Location"] = url + "?andthenanotherthing"
        ret[1]["Server"] = "AIStore"
        ret = tuple(ret)
    return ret


@wrapt.when_imported("moto.s3.responses")
def patch_moto(module):  # pylint: disable=unused-variable
    """
    Meta-mock our moto mocks to make them send redirects on occasion.

    Bucket delete won't play nicely. Others are more helpful.
    """
    for func in test_ops:
        logging.debug("Patching S3Response.%s", func)
        wrapt.wrap_function_wrapper(module, "S3Response." + func, s3_response_wrapper)
