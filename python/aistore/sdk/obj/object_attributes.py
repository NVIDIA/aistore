#
# Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
#

from dataclasses import dataclass
from typing import Dict, Optional
from requests.structures import CaseInsensitiveDict
from aistore.sdk.const import (
    HEADER_CONTENT_LENGTH,
    AIS_CHECKSUM_TYPE,
    AIS_CHECKSUM_VALUE,
    AIS_ACCESS_TIME,
    AIS_VERSION,
    AIS_CUSTOM_MD,
    AIS_PRESENT,
    AIS_CHUNKS_COUNT,
    AIS_CHUNKS_MAX_CHUNK_SIZE,
    HEADER_LAST_MODIFIED,
    HEADER_ETAG,
)


# pylint: disable=too-few-public-methods
class ObjectAttributes:
    """
    Represents the attributes parsed from the response headers returned from an API call to get an object.

    Args:
        response_headers (CaseInsensitiveDict): Response header dict containing object attributes
    """

    def __init__(self, response_headers: CaseInsensitiveDict):
        self._response_headers = response_headers

    @property
    def size(self) -> int:
        """
        Size of object content.
        """
        return int(self._response_headers.get(HEADER_CONTENT_LENGTH, 0))

    @property
    def checksum_type(self) -> str:
        """
        Type of checksum, e.g. xxhash or md5.
        """
        return self._response_headers.get(AIS_CHECKSUM_TYPE, "")

    @property
    def checksum_value(self) -> str:
        """
        Checksum value.
        """
        return self._response_headers.get(AIS_CHECKSUM_VALUE, "")

    @property
    def access_time(self) -> str:
        """
        Time this object was accessed.
        """
        return self._response_headers.get(AIS_ACCESS_TIME, "")

    @property
    def obj_version(self) -> str:
        """
        Object version.
        """
        return self._response_headers.get(AIS_VERSION, "")

    @property
    def custom_metadata(self) -> Dict[str, str]:
        """
        Dictionary of custom metadata.
        """
        custom_md_header = self._response_headers.get(AIS_CUSTOM_MD, "")
        if len(custom_md_header) > 0:
            return self._parse_custom(custom_md_header)
        return {}

    @property
    def present(self) -> bool:
        """
        Whether the object is present/cached.
        """
        return self._response_headers.get(AIS_PRESENT, "") == "true"

    @staticmethod
    def _parse_custom(custom_md_header) -> Dict[str, str]:
        """
        Parse the comma-separated list of optional custom metadata from the custom metadata header.

        Args:
            custom_md_header: Header containing metadata csv

        Returns:
            Dictionary of custom metadata
        """
        custom_metadata = {}
        for entry in custom_md_header.split(","):
            try:
                assert isinstance(entry, str)
                entry_list = entry.strip().split("=")
                assert len(entry_list) == 2
                custom_metadata[entry_list[0]] = entry_list[1]
            except AssertionError:
                continue
        return custom_metadata


@dataclass
class ChunksInfo:
    """
    Information about chunked object storage.

    Attributes:
        chunk_count: Number of chunks the object is split into.
        max_chunk_size: Size of the largest chunk in bytes.
    """

    chunk_count: int = 0
    max_chunk_size: int = 0


class ObjectAttributesV2(ObjectAttributes):
    """
    Extended object attributes returned from HeadObjectV2 API.

    This class extends ObjectAttributes with V2-specific fields like
    chunk information, last modified time, and ETag.

    Args:
        response_headers (CaseInsensitiveDict): Response header dict containing object attributes
    """

    @property
    def last_modified(self) -> str:
        """
        Last modification time of the object (RFC1123 format).
        """
        return self._response_headers.get(HEADER_LAST_MODIFIED, "")

    @property
    def etag(self) -> str:
        """
        Entity tag (ETag) of the object.
        """
        return self._response_headers.get(HEADER_ETAG, "").strip('"')

    @property
    def chunks(self) -> Optional[ChunksInfo]:
        """
        Chunk information for chunked objects.

        Returns:
            ChunksInfo if object is chunked, None otherwise.
        """
        count_str = self._response_headers.get(AIS_CHUNKS_COUNT, "")
        max_size_str = self._response_headers.get(AIS_CHUNKS_MAX_CHUNK_SIZE, "")

        if not count_str and not max_size_str:
            return None

        return ChunksInfo(
            chunk_count=int(count_str) if count_str else 0,
            max_chunk_size=int(max_size_str) if max_size_str else 0,
        )
