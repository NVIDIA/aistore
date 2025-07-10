#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import json
import base64
from typing import Optional, List, Dict, Tuple

from pydantic.v1 import BaseModel, Field

from aistore.sdk.obj.object import Object
from aistore.sdk.const import (
    GB_OBJNAME,
    GB_BCK,
    GB_PROVIDER,
    GB_UNAME,
    GB_ARCHPATH,
    GB_START,
    GB_LENGTH,
    GB_OPAQUE,
    GB_IN,
    GB_OUTPUT_FMT,
    GB_CONTINUE_ERR,
    GB_ONLY_OBJ_NAME,
    GB_STRM_GET,
    EXT_TAR,
)


# See api/apc/ml.go for the Go Moss API
class BatchRequest:
    """
    Represents a batch request for retrieving or processing multiple objects from an AIStore cluster.

    A `BatchRequest` allows you to specify a set of objects (optionally including archive members, metadata, or byte ranges)
    and configure how the batch operation should behave. This class is used to build up the request before
    sending it to the cluster via the `BatchLoader` class.

    Args:
        output_format (str, optional): Format for the batch response output. Determines how the
            retrieved objects are packaged and returned. Supported formats include .tar.gz, .tgz,
            and .tar. Defaults to .tar.

        continue_on_err (bool, optional): Whether to continue processing remaining objects in the
            batch if an error occurs with one object. When True, errors for individual objects
            are reported but don't stop the entire batch operation. These will be reported in
            `BatchResponseItem.missing`. When False, the first error encountered will halt the
            entire batch request. Defaults to True.

        only_obj_name (bool, optional): By default, the naming convention is
            name-in-archive (<Bucket>/<ObjName>). If True set this flag to have <ObjName> only.
            Defaults to False.

        streaming (bool, optional): Enables streaming mode for the batch request. When True,
            objects are processed and returned as a stream containing an archive.
            This is beneficial for large batches or when processing time varies
            significantly between objects. When False, the response is not streamed
            and will include more metadata per item. Defaults to True.

    Note:
        - The batch request is passed to the `BatchLoader`, which performs the actual HTTP request.
        - This class does not make any network calls itself.
    """

    def __init__(
        self,
        output_format: str = EXT_TAR,
        continue_on_err: bool = True,
        only_obj_name: bool = False,
        streaming: bool = True,
    ):
        self.obj_requests: List[BatchObjectRequest] = []
        self.output_format = output_format
        self.continue_on_err = continue_on_err
        self.only_obj_name = only_obj_name
        self.streaming = streaming

    def add_object_request(
        self,
        obj: Object,
        opaque: Optional[bytes] = None,
        archpath: Optional[str] = None,
        start: Optional[int] = None,
        length: Optional[int] = None,
    ) -> None:
        """
        Add a single object request to the `BatchRequest`.
        This function does not make an HTTP call.

        Args:
            obj (Object): AIStore object instance containing bucket and object name information.
                The object's name, bucket name, and provider will be used in the batch request.

            opaque (Optional[bytes], optional): User-provided binary identifier for maintaining
                one-to-many relationships or custom metadata. This data is base64-encoded and
                returned unchanged in the response for client-side correlation. Defaults to None.

            archpath (Optional[str], optional): Path to a specific file within an archived object.
                Used to extract individual files from archives (.tar, .tgz, .tar.gz, .zip, .tar.lz4).
                For example, if obj is "dataset.tar" and archpath is "images/photo1.jpg",
                only that specific file will be extracted from the archive. Defaults to None.

            start (Optional[int], optional): Starting byte offset for partial object retrieval.
                When specified with length, only a range of bytes will be retrieved instead
                of the entire object. Useful for large files or streaming scenarios.
                Defaults to None (retrieve from beginning).

            length (Optional[int], optional): Number of bytes to retrieve starting from the
                start offset. Must be used together with start parameter. When None,
                retrieves from start offset to end of object. Defaults to None.

        Returns:
            None
        """
        request_data = {
            GB_OBJNAME: obj.name,
            GB_BCK: obj.bucket_name,
            GB_PROVIDER: obj.bucket_provider.value,
            GB_UNAME: "",
        }

        if opaque is not None:
            request_data[GB_OPAQUE] = base64.b64encode(opaque).decode()
        if archpath is not None:
            request_data[GB_ARCHPATH] = archpath
        if start is not None:
            request_data[GB_START] = start
        if length is not None:
            request_data[GB_LENGTH] = length

        # Create and append (still using Pydantic BatchObjectRequest)
        batch_request = BatchObjectRequest(**request_data)
        self.obj_requests.append(batch_request)

    def is_empty(self) -> bool:
        """Check if the request is empty."""
        return not self.obj_requests

    def get_request_bck_name_prov(self) -> Tuple[str, str]:
        """Get bucket name and provider of first object request."""
        if self.obj_requests:
            return self.obj_requests[0].bck, self.obj_requests[0].provider
        raise ValueError("BatchRequest is empty")

    def to_dict(self) -> Dict:
        """Convert to dictionary using field aliases."""
        return {
            GB_IN: [
                req.dict() for req in self.obj_requests
            ],  # Use Pydantic's dict() method
            GB_OUTPUT_FMT: self.output_format,
            GB_CONTINUE_ERR: self.continue_on_err,
            GB_ONLY_OBJ_NAME: self.only_obj_name,
            GB_STRM_GET: self.streaming,
        }

    def to_json(self) -> str:
        """Serialize to JSON string."""
        return json.dumps(self.to_dict(), indent=4)

    @classmethod
    def from_json(cls, json_str: str) -> "BatchRequest":
        """
        Parse a BatchRequest object from a JSON string.

        Args:
            json_str (str): JSON string representing the batch request.

        Returns:
            BatchRequest: A parsed instance of the batch request.
        """
        data = json.loads(json_str)

        # Create instance with main fields
        instance = cls(
            output_format=data.get(GB_OUTPUT_FMT, EXT_TAR),
            continue_on_err=data.get(GB_CONTINUE_ERR, True),
            only_obj_name=data.get(GB_ONLY_OBJ_NAME, False),
            streaming=data.get(GB_STRM_GET, True),
        )

        # Add object requests using Pydantic BatchObjectRequest
        for req_data in data.get(GB_IN, []):
            batch_request = BatchObjectRequest(**req_data)
            instance.obj_requests.append(batch_request)

        return instance


class BatchObjectRequest(BaseModel):
    """Single object request within a batch."""

    obj_name: str = Field(alias=GB_OBJNAME)
    bck: str = Field(default="", alias=GB_BCK)
    provider: str = Field(default="", alias=GB_PROVIDER)
    uname: str = Field(default="", alias=GB_UNAME)
    opaque: Optional[str] = Field(default=None, alias=GB_OPAQUE)
    archpath: Optional[str] = Field(default="", alias=GB_ARCHPATH)
    start: Optional[int] = Field(default=None, alias=GB_START)
    length: Optional[int] = Field(default=None, alias=GB_LENGTH)

    def dict(self, *_args, **_kwargs) -> Dict:
        """Override Pydantic dict to use aliases and exclude default fields from request."""
        return super().dict(by_alias=True, exclude_defaults=True)

    class Config:
        allow_population_by_field_name = True
