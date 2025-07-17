#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import json
import base64
from typing import Optional, List

from pydantic.v1 import BaseModel, Field, validator

from aistore.sdk.const import (
    GB_OBJNAME,
    GB_BCK,
    GB_PROVIDER,
    GB_ARCHPATH,
    GB_OPAQUE,
    GB_ERR_MSG,
    GB_SIZE,
    GB_OUT,
    GB_UUID,
)
from aistore.sdk.batch.batch_request import BatchRequest


# See api/apc/ml.go for the Go Moss API
class BatchResponseItem(BaseModel):
    """
    Represents a single object response within a batch operation from an AIStore cluster.

    This class contains the metadata and status information for one object that was processed
    as part of a batch request. Each `BatchResponseItem` corresponds to one object request
    that was submitted in the original `BatchRequest`. A streaming `BatchRequest`
    will populate more of the metadata in the response item.

    Args:
        obj_name (str): Name of the object that was processed.

        bucket (str): Name of the bucket containing the object.

        provider (str): Storage provider identifier (e.g., 'ais', 'aws', 'gcp') for the
            bucket where the object resides.

        archpath (str): Path to a specific file within an archived object if the request
            targeted a file inside a supported archive format. Empty string if the
            entire object was requested. Defaults to empty string.

        opaque (Optional[bytes]): User-provided binary identifier that was included in the
            original request, returned unchanged for client-side correlation. Defaults to None.

        err_msg (Optional[str]): Error message if the object processing failed. None if
            the operation was successful. Defaults to None.

        size (int): Size of the processed object in bytes. For archive paths, this represents
            the size of the extracted file rather than the entire archive. Defaults to 0.

        is_missing (bool): Internal flag indicating whether the requested object or archive
            member was not found. Defaults to False.
    """

    obj_name: str = Field(default="", alias=GB_OBJNAME)
    bucket: str = Field(default="", alias=GB_BCK)
    provider: str = Field(default="", alias=GB_PROVIDER)
    archpath: str = Field(default="", alias=GB_ARCHPATH)
    opaque: Optional[bytes] = Field(default=None, alias=GB_OPAQUE)
    err_msg: Optional[str] = Field(default=None, alias=GB_ERR_MSG)
    size: int = Field(default=0, alias=GB_SIZE)

    # Deviation from Go API, handle empty file
    is_missing: bool = Field(default=False)

    # pylint: disable=too-few-public-methods
    class Config:
        """
        BaseModel.Config field setting.
        """

        allow_population_by_field_name = True

    @classmethod
    def from_batch_request(
        cls, batch_request: BatchRequest, req_index: int
    ) -> "BatchResponseItem":
        """
        Parse a BatchResponseItem object from BatchRequest instance using the provided index.

        Args:
            batch_request (BatchRequest): BatchRequest to populate data from
            req_index (int): Index of object requests within the BatchRequest

        Returns:
            BatchResponseItem: A parsed instance of the batch response item.
        """
        return cls(**batch_request.obj_requests[req_index].dict())

    # pylint: disable=no-self-argument
    @validator("opaque", pre=True)
    def decode_opaque(cls, opaque_val):
        """
        Automatically decode base64 encoded opaque field back to bytes.

        Args:
            opaque_val: The raw opaque value (string or bytes)

        Returns:
            Optional[bytes]: Decoded bytes or None
        """
        if opaque_val is None:
            return None
        # Already decoded
        if isinstance(opaque_val, bytes):
            return opaque_val
        if isinstance(opaque_val, str) and opaque_val:
            try:
                return base64.urlsafe_b64decode(opaque_val)
            except Exception:
                # Handle invalid base64 gracefully
                return None
        return None


class BatchResponse(BaseModel):
    """
    Represents a batch response with per-object metadata. Internal class.
    """

    responses: List[BatchResponseItem] = Field(default_factory=list)
    uuid: str = Field(default="")

    @classmethod
    def from_json(cls, json_str: str) -> "BatchResponse":
        """
        Parse a BatchResponse object from a JSON string.

        Args:
            json_str (str): JSON string representing the batch response.

        Returns:
            BatchResponse: A parsed instance of the batch response.
        """
        data = json.loads(json_str)

        # Create BatchResponseItem instances from the raw data
        response_items = [BatchResponseItem(**item) for item in data.get(GB_OUT, [])]

        return cls(responses=response_items, uuid=data.get(GB_UUID, ""))
