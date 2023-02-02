#
# Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=missing-module-docstring
import io
import logging
import unittest

import boto3
from moto import mock_s3
from botocore.exceptions import ClientError

from tests import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_SESSION_TOKEN,
    AWS_DEFAULT_REGION,
)
from tests.utils import random_string
from tests.unit.botocore_patch import mock_s3_redirect


# pylint: disable=too-many-instance-attributes,missing-function-docstring,invalid-name,unused-variable
class BotocoreBaseTest(unittest.TestCase):
    """
    Common test group for the botocore monkey patch;
    Runs a small set of S3 operations.

    We run this over and over, varying whether redirects
    are issued, and whether our monkey patch is loaded
    to handle them.

    If botocore has been monkeypatched, it should
    not get upset when redirected.

    If botocore has not, it should get upset every time.

    For units, we use moto to mock an S3 instance; to
    control whether redirects are issued we use a
    decorator (see mock_s3_redirect.py).

    To control botocore's expected behavior we use the
    redirect_errors_expected property.
    """

    __test__ = False
    mock_s3 = mock_s3()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.enable_redirects = False
        self.redirect_errors_expected = False

        # Use moto to mock S3 by default.
        self.use_moto = True
        # AIstore endpoint URL to use iff we're not using moto.
        self.endpoint_url = kwargs.get("endpoint_url", "http://localhost:8080/s3")

    def setUp(self):
        self.control_bucket = random_string()
        self.control_object = random_string()
        self.another_bucket = random_string()
        self.another_object = random_string()

        if self.use_moto:
            logging.debug("Using moto for S3 services")
            # Disable any redirections until we're ready.
            mock_s3_redirect.redirections_enabled = False
            self.mock_s3.start()
            self.s3 = boto3.client(
                "s3", region_name="us-east-1"
            )  # pylint: disable=invalid-name
        else:
            logging.debug("Using aistore for S3 services")
            self.s3 = boto3.client(
                "s3",
                region_name=AWS_DEFAULT_REGION,
                endpoint_url=self.endpoint_url,
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN,
            )

        self.s3.create_bucket(Bucket=self.control_bucket)
        self.s3.upload_fileobj(
            io.BytesIO(b"Hello, world!"), self.control_bucket, self.control_object
        )

        if self.use_moto:
            # Enable redirections if we've been asked to do so.
            mock_s3_redirect.redirections_enabled = self.enable_redirects

    def tearDown(self):
        # pylint: disable=broad-exception-raised
        if self.use_moto:
            self.mock_s3.stop()
        else:
            try:
                self.s3.delete_object(
                    Bucket=self.control_bucket, Key=self.control_object
                )
            except Exception:
                pass
            try:
                self.s3.delete_bucket(Bucket=self.control_bucket)
            except Exception:
                pass
            try:
                self.s3.delete_object(
                    Bucket=self.control_bucket, Key=self.another_object
                )
            except Exception:
                pass
            try:
                self.s3.delete_bucket(Bucket=self.another_bucket)
            except Exception:
                pass

    def test_bucket_create(self):
        # When integration testing against a real aistore, this won't redirect.
        redirect_errors_expected = (
            False if not self.use_moto else self.redirect_errors_expected
        )

        with MightRedirect(redirect_errors_expected, operation="_bucket_response_put"):
            logging.warning("Creating bucket %s", self.another_bucket)
            self.s3.create_bucket(Bucket=self.another_bucket)

    def test_bucket_list(self):
        # Our redirect mock can't intercept bucket listing operations;
        # so, always expect success
        self.assertIn(
            self.control_bucket, [b["Name"] for b in self.s3.list_buckets()["Buckets"]]
        )

    def test_object_create(self):
        with MightRedirect(self.redirect_errors_expected):
            self.s3.upload_fileobj(
                io.BytesIO(b"Hello, world!"), self.control_bucket, self.another_object
            )

    def test_object_list(self):
        # Our redirect mock can't intercept object listing operations;
        # so, always expect success
        self.assertEqual(
            [
                b["Key"]
                for b in self.s3.list_objects(Bucket=self.control_bucket)["Contents"]
            ],
            [self.control_object],
        )

    def test_object_get(self):
        with MightRedirect(self.redirect_errors_expected):
            stream_str = io.BytesIO()
            self.s3.download_fileobj(
                self.control_bucket, self.control_object, stream_str
            )
            self.assertEqual(stream_str.getvalue().decode("utf-8"), "Hello, world!")

    def test_caching(self):
        with MightRedirect(self.redirect_errors_expected):
            stream_str = io.BytesIO()
            self.s3.download_fileobj(
                self.control_bucket, self.control_object, stream_str
            )
            self.assertEqual(stream_str.getvalue().decode("utf-8"), "Hello, world!")

            self.s3.download_fileobj(
                self.control_bucket, self.control_object, stream_str
            )
            self.assertEqual(stream_str.getvalue().decode("utf-8"), "Hello, world!")

    def test_object_delete(self):
        with MightRedirect(self.redirect_errors_expected):
            self.s3.delete_object(Bucket=self.control_bucket, Key=self.control_object)

    def test_bucket_delete(self):
        with MightRedirect(self.redirect_errors_expected):
            self.s3.delete_object(Bucket=self.control_bucket, Key=self.control_object)
            self.s3.delete_bucket(Bucket=self.control_bucket)


class MightRedirect:
    """
    Context manager to handle botocore errors.

    Some test sets expect botocore to issue errors
    when it encounters redirects. Others expect
    the opposite.

    This allows us to control the expected behavior.
    """

    max_retries = 3

    def __init__(self, redirect_errors_expected=False, operation=None):
        self.redirect_errors_expected = redirect_errors_expected
        self.operation = operation

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        if self.redirect_errors_expected:
            try:
                if exc and value:
                    raise value
            except ClientError as ex:
                # Some operations don't pass through redirect errors directly
                if self.operation in ["_bucket_response_put"]:
                    return True
                if int(ex.response["Error"]["Code"]) in [302, 307]:
                    return True
            instead = "No error"
            if value:
                instead = value

            # pylint: disable=broad-exception-raised
            raise Exception(
                "A ClientError with a redirect code was expected, "
                + "but didn't happen. Instead: "
                + instead
            )
        return False
