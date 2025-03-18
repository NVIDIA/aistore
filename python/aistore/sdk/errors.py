#
# Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
#


class APIRequestError(Exception):
    """
    Base class for errors from HTTP servers, e.g. AIS or AuthN
    """

    def __init__(self, status_code: int, message: str, req_url: str):
        self.status_code = status_code
        self.message = message
        self.req_url = req_url  # Standardize attribute name across errors
        super().__init__(f"STATUS:{status_code}, MESSAGE:{message}, REQ_URL:{req_url}")


class AISError(APIRequestError):
    """
    Raised when an error is encountered from a query to the AIS cluster
    """


class AISRetryableError(AISError):
    """
    Exception raised for AIStore related errors that may resolve by retrying.
    """


# pylint: disable=unused-variable
class InvalidBckProvider(Exception):
    """
    Raised when the bucket provider is invalid for the requested operation
    """

    def __init__(self, provider):
        super().__init__(f"Invalid bucket provider: '{provider}'")


# pylint: disable=unused-variable
class ErrRemoteBckNotFound(AISError):
    """
    Raised when a remote bucket its required and missing for the requested operation
    """


# pylint: disable=unused-variable
class ErrBckNotFound(AISError):
    """
    Raised when a bucket is expected and not found
    """


# pylint: disable=unused-variable
class ErrObjNotFound(AISRetryableError):
    """
    Raised when an object is expected and not found
    """


# pylint: disable=unused-variable
class ErrBckAlreadyExists(AISError):
    """
    Raised when a bucket is created but already exists in AIS
    """


# pylint: disable=unused-variable
class ErrETLAlreadyExists(AISError):
    """
    Raised when an ETL is created but already exists in AIS
    """


class ErrGETConflict(AISRetryableError):
    """
    Raised when an ETL is created but already exists in AIS
    """


# pylint: disable=unused-variable
class ErrETLNotFound(AISError):
    """
    Raised when an ETL is expected but not found
    """


# pylint: disable=unused-variable
class Timeout(Exception):
    """
    Raised when an operation takes too long to complete
    """

    def __init__(self, action, message=""):
        super().__init__(f"Timed out while waiting for {action}. {message}")


# pylint: disable=unused-variable
class InvalidObjectRangeIndex(Exception):
    """
    Raised when incorrect range parameters are passed when creating an ObjectRange
    """

    def __init__(self, message):
        super().__init__(f"Invalid argument provided for object range index: {message}")


class JobInfoNotFound(Exception):
    """
    Raised when information on a job's status could not be found on the AIS cluster
    """

    def __init__(self, message):
        super().__init__(f"Job information not found on the cluster: {message}")


class UnexpectedHTTPStatusCode(Exception):
    """
    Raised when the status code from a response is not what's expected.
    """

    def __init__(self, expected_status_codes, received_status_code):
        expected_codes = ", ".join(str(code) for code in expected_status_codes)
        super().__init__(
            (
                f"Unexpected status code received. "
                f"Expected one of the following: {expected_codes}, "
                f"but received: {received_status_code}"
            )
        )


class InvalidURLException(Exception):
    """
    Raised when the URL is invalid or any part of it is missing.
    """

    def __init__(self, url):
        super().__init__(
            f"Invalid URL: '{url}'. Ensure it follows the format 'provider://bucket/object'."
        )


class NoTargetError(Exception):
    """
    Raised when attempting to select a target for an object, but none were found in cluster map
    """

    def __init__(self, total_nodes: int):
        super().__init__(
            f"No available targets in the cluster map. Total nodes: {total_nodes}"
        )
