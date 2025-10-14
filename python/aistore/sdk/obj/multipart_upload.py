#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import List, Dict

from requests import Response
from aistore.sdk.request_client import RequestClient
from aistore.sdk.obj.object_writer import ObjectWriter
from aistore.sdk.const import (
    HTTP_METHOD_POST,
    HTTP_METHOD_DELETE,
    QPARAM_MPT_UPLOAD_ID,
    QPARAM_MPT_PART_NO,
    ACT_MPT_UPLOAD,
    ACT_MPT_COMPLETE,
    ACT_MPT_ABORT,
)
from aistore.sdk.types import ActionMsg, MptCompletedPart


class MultipartUpload:
    """
    Provides methods for multipart upload operations in AIS.

    Args:
        client (RequestClient): Client used for all http requests.
        object_path (str): Path to the object being uploaded.
    """

    def __init__(
        self, client: RequestClient, object_path: str, params: Dict[str, str] = None
    ):
        self.client = client
        self.object_path = object_path
        self.params = params
        self.upload_id = None
        self.parts: List[int] = []

    def create(self) -> "MultipartUpload":
        """
        Create a multipart upload session.

        Returns:
            MultipartUpload: Self for method chaining.
        """
        self.upload_id = self.client.request(
            HTTP_METHOD_POST,
            path=self.object_path,
            params=self.params,
            json=ActionMsg(action=ACT_MPT_UPLOAD).model_dump(),
        ).text
        return self

    def add_part(self, part_number: int) -> ObjectWriter:
        """
        Add a part to the multipart upload.

        Args:
            part_number (int): The part number for this part.

        Returns:
            ObjectWriter: Writer for uploading the part content.

        Raises:
            ValueError: If multipart upload has not been created or if part_number is not a positive integer.
        """
        if self.upload_id is None:
            raise ValueError("Multipart upload not created")
        if not isinstance(part_number, int) or part_number <= 0:
            raise ValueError("Part number must be a positive integer")
        self.parts.append(part_number)
        params = self.params or {}
        return ObjectWriter(
            self.client,
            self.object_path,
            {
                **params,
                QPARAM_MPT_UPLOAD_ID: self.upload_id,
                QPARAM_MPT_PART_NO: part_number,
            },
        )

    def complete(self) -> Response:
        """
        Complete the multipart upload.

        Returns:
            Response: HTTP response from the server.
        """
        params = self.params or {}
        return self.client.request(
            HTTP_METHOD_POST,
            path=self.object_path,
            params={**params, QPARAM_MPT_UPLOAD_ID: self.upload_id},
            json=ActionMsg(
                action=ACT_MPT_COMPLETE,
                value=[
                    MptCompletedPart(part_number=part_number, etag="").as_dict()
                    for part_number in self.parts
                ],
            ).model_dump(),
        )

    def abort(self) -> Response:
        """
        Abort the multipart upload.

        Returns:
            Response: HTTP response from the server.
        """
        params = self.params or {}
        return self.client.request(
            HTTP_METHOD_DELETE,
            path=self.object_path,
            params={**params, QPARAM_MPT_UPLOAD_ID: self.upload_id},
            json=ActionMsg(action=ACT_MPT_ABORT).model_dump(),
        )
