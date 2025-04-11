#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from abc import ABC, abstractmethod
from typing import Optional, Type

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
        raise self.exc_class(r.status_code, r.text, r.request.url or "")

    @abstractmethod
    def parse_error(self, r: requests.Response) -> APIRequestError:
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
    def exc_class(self) -> Type[AISError]:
        return AISError

    def parse_error(self, r: requests.Response) -> AISError:
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
        prov, bck, has_obj = extract_and_parse_url(message) or (None, None, None)

        if prov is not None:
            try:
                prov = Provider.parse(prov)
            except InvalidBckProvider:
                prov = None

        is_remote_alias_bck = bck is not None and "@" in bck
        is_remote_bck = (prov is not None) and (prov.is_remote() or is_remote_alias_bck)
        is_etl_err = "etl job" in message
        is_get_request = r.request.method == "GET"

        exc = self.exc_class
        if status == 404:
            exc = self._parse_404(is_etl_err, is_remote_bck, prov, has_obj) or exc
        elif status == 409:
            exc = self._parse_409(is_etl_err, is_get_request, prov, has_obj) or exc

        return exc(status, message, req_url or "")

    @staticmethod
    def _parse_404(
        is_etl_err: bool,
        is_remote_bck: Optional[bool] = None,
        provider: Optional[Provider] = None,
        has_obj: Optional[bool] = None,
    ) -> Optional[Type[AISError]]:
        if is_etl_err:
            return ErrETLNotFound
        if provider:
            if has_obj:
                return ErrObjNotFound
            if is_remote_bck:
                return ErrRemoteBckNotFound
            return ErrBckNotFound
        return None

    @staticmethod
    def _parse_409(
        is_etl_err: bool,
        is_get_request: bool,
        provider: Optional[Provider] = None,
        has_obj: Optional[bool] = None,
    ) -> Optional[Type[AISError]]:
        if is_get_request:
            return ErrGETConflict
        if is_etl_err:
            return ErrETLAlreadyExists
        if provider and not has_obj:
            return ErrBckAlreadyExists
        return None
