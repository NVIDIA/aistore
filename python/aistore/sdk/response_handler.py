#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from abc import ABC, abstractmethod
from typing import Type

import requests

from aistore.sdk.errors import (
    AISError,
    ErrETLNotFound,
    ErrObjNotFound,
    ErrRemoteBckNotFound,
    ErrBckNotFound,
    ErrETLAlreadyExists,
    ErrBckAlreadyExists,
    InvalidBckProvider,
    APIRequestError,
    ErrGETConflict,
)
from aistore.sdk.provider import Provider
from aistore.sdk.utils import extract_and_parse_url


class ResponseHandler(ABC):
    """
    Abstract base class for handling HTTP API responses
    """

    # pylint: disable=unused-argument
    def handle_response(self, r: requests.Response) -> requests.Response:
        """
        Method compatible with `requests` hook signature to process an HTTP response

        Args:
            r (requests.Response): Response from the server
        """
        if 200 <= r.status_code < 400:
            return r
        # Raise wrapped HTTPError if response indicates
        try:
            r.raise_for_status()
        except requests.exceptions.HTTPError as http_err:
            # Raise specific error type but keep full HTTPError with stack trace
            raise self.parse_error(r) from http_err
        # If not an expected status code, raise general error
        raise self.exc_class(r.status_code, r.text, r.request.url)

    @abstractmethod
    def parse_error(self, r: requests.Response) -> Exception:
        """Parse custom exception from failed response (must be implemented)"""

    @property
    @abstractmethod
    def exc_class(self) -> Type[APIRequestError]:
        """Exception class for generic error handling (must be implemented)"""


class AISResponseHandler(ResponseHandler):
    """
    Handle responses from an AIS cluster
    """

    @property
    def exc_class(self) -> Type[APIRequestError]:
        return AISError

    def parse_error(self, r: requests.Response) -> Exception:
        """
        Parse response contents to raise an appropriate AISError object.

        Args:
            r (requests.Response): HTTP status code of an HTTPError returned from AIS

        Returns:
            AISError: If the error doesn't match any specific conditions.
            ErrBckNotFound: If the error message indicates a missing bucket.
            ErrRemoteBckNotFound: If the error message indicates a missing remote bucket.
            ErrObjNotFound: If the error message indicates a missing object.
            ErrBckAlreadyExists: If the error message indicates a bucket already exists.
            ErrETLAlreadyExists: If the error message indicates an ETL already exists
            ErrETLNotFound: If the error message indicates a missing ETL.
        """
        status, message, req_url = r.status_code, r.text, r.request.url
        prov, bck, obj = extract_and_parse_url(message) or (None, None, None)
        if prov:
            try:
                prov = Provider.parse(prov)
            except InvalidBckProvider:
                prov = None

        exc_class = self.exc_class
        if status == 404:
            if "etl job" in message:
                exc_class = ErrETLNotFound
            elif prov:
                if obj:
                    exc_class = ErrObjNotFound
                elif prov.is_remote() or "@" in bck:
                    exc_class = ErrRemoteBckNotFound
                else:
                    exc_class = ErrBckNotFound
        elif status == 409:
            if "etl job" in message:
                exc_class = ErrETLAlreadyExists
            elif prov and not obj:
                exc_class = ErrBckAlreadyExists
            elif (
                r.request.method == "GET"
            ):  # Only raise ErrGETConflict for GET requests
                exc_class = ErrGETConflict

        return exc_class(status, message, req_url)
