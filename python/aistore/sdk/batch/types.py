#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

import base64
import binascii
from typing import Optional, List, Dict

from pydantic import BaseModel, Field, field_validator

from aistore.sdk.const import (
    GB_ERR_MSG,
    GB_OBJNAME,
    GB_BCK,
    GB_OUT,
    GB_PROVIDER,
    GB_UNAME,
    GB_OPAQUE,
    GB_ARCHPATH,
    GB_START,
    GB_LENGTH,
    GB_IN,
    GB_OUTPUT_FMT,
    GB_CONTINUE_ERR,
    GB_ONLY_OBJ_NAME,
    GB_STRM_GET,
    GB_SIZE,
    GB_UUID,
)


# pylint: disable=too-few-public-methods
class MossIn(BaseModel):
    """Matches apc.MossIn in Go."""

    obj_name: str = Field(alias=GB_OBJNAME)  # name of the object
    # Optional fields
    bck: Optional[str] = Field(
        default=None, alias=GB_BCK
    )  # if present, overrides cmn.Bck from the GetBatch request
    provider: Optional[str] = Field(
        default=None, alias=GB_PROVIDER
    )  # e.g. "s3", "ais", etc.
    uname: Optional[str] = Field(
        default=None, alias=GB_UNAME
    )  # per-object, fully qualified - defines the entire (bucket, provider, objname) triplet, and more
    opaque: Optional[str] = Field(
        default=None, alias=GB_OPAQUE
    )  # user-provided identifier - e.g., to maintain one-to-many
    archpath: Optional[str] = Field(
        default=None, alias=GB_ARCHPATH
    )  # extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
    start: Optional[int] = Field(
        default=None, alias=GB_START
    )  # start offset in the object
    length: Optional[int] = Field(
        default=None, alias=GB_LENGTH
    )  # length of the data to be extracted

    model_config = {"populate_by_name": True}

    def dict(self, *_args, **_kwargs) -> Dict:
        return self.model_dump(by_alias=True, exclude_defaults=True)


# pylint: disable=too-few-public-methods
class MossOut(BaseModel):
    """MOSS (Multi-Object Streaming Service) response. Matches apc.MossOut in Go - Response for each object"""

    obj_name: str = Field(alias=GB_OBJNAME)  # name of the object
    archpath: Optional[str] = Field(
        default=None, alias=GB_ARCHPATH
    )  # path of the object in the shard
    bucket: str = Field(alias=GB_BCK)
    provider: str = Field(alias=GB_PROVIDER)
    opaque: Optional[bytes] = Field(default=None, alias=GB_OPAQUE)
    err_msg: Optional[str] = Field(default=None, alias=GB_ERR_MSG)
    size: int = Field(default=0, alias=GB_SIZE)

    model_config = {"populate_by_name": True}

    def dict(self, *_args, **_kwargs) -> Dict:
        return self.model_dump(by_alias=True, exclude_defaults=True)

    # pylint: disable=no-self-argument
    @field_validator("opaque", mode="before")
    def decode_opaque(cls, opaque_val):
        """
        Automatically decode base64 encoded opaque field back to bytes.

        Args:
            opaque_val (Union[str, bytes, None]): The raw opaque value. Can be a base64-encoded
                string, already decoded bytes, or None if no opaque data was provided.

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
            except (binascii.Error, ValueError):
                # Handle invalid base64 gracefully
                return None
        return None


# pylint: disable=too-few-public-methods
class MossReq(BaseModel):
    """MOSS (Multi-Object Streaming Service) request. Matches apc.MossReq in Go."""

    moss_in: List[MossIn] = Field(default_factory=list, alias=GB_IN)  # MossIn Requests
    # Optional fields
    output_format: Optional[str] = Field(
        default=None, alias=GB_OUTPUT_FMT
    )  # tar, tgz, zip, etc.
    cont_on_err: Optional[bool] = Field(
        default=None, alias=GB_CONTINUE_ERR
    )  # primary usage: ignore missing files and/or objects - include them under "__404__/" prefix and keep going
    only_obj_name: Optional[bool] = Field(
        default=None, alias=GB_ONLY_OBJ_NAME
    )  # name-in-archive: default naming convention is <Bucket>/<ObjName>; set this flag to have <ObjName> only
    streaming_get: Optional[bool] = Field(
        default=None, alias=GB_STRM_GET
    )  # stream resulting archive prior to finalizing it in memory

    model_config = {"populate_by_name": True}

    def dict(self, *_args, **_kwargs) -> Dict:
        return self.model_dump(by_alias=True, exclude_defaults=True)

    def add(self, moss_in: MossIn) -> "MossReq":
        """
        Add a MossIn request. Returns self for chaining.

        Args:
            moss_in (MossIn): MossIn object to add to the request

        Returns:
            MossReq: Self for method chaining
        """
        self.moss_in.append(moss_in)  # pylint: disable=no-member
        return self

    def extend(self, moss_ins: List[MossIn]) -> "MossReq":
        """
        Add multiple MossIn requests. Returns self for chaining.

        Args:
            moss_ins (List[MossIn]): List of MossIn objects to add

        Returns:
            MossReq: Self for method chaining
        """
        self.moss_in.extend(moss_ins)  # pylint: disable=no-member
        return self


# pylint: disable=too-few-public-methods
class MossResp(BaseModel):
    """Matches apc.MossResp in Go"""

    out: List[MossOut] = Field(default_factory=list, alias=GB_OUT)
    uuid: str = Field(default="", alias=GB_UUID)

    model_config = {"populate_by_name": True}
