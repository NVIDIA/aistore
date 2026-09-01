#
# Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
#

from dataclasses import dataclass
from typing import Dict, List, Optional

from requests.structures import CaseInsensitiveDict

from aistore.sdk.const import (
    AIS_ACCESS_TIME,
    AIS_CHECKSUM_TYPE,
    AIS_CHECKSUM_VALUE,
    AIS_CHUNKS_COUNT,
    AIS_CHUNKS_MAX_CHUNK_SIZE,
    AIS_CUSTOM_MD,
    AIS_EC_DATA,
    AIS_EC_GENERATION,
    AIS_EC_PARITY,
    AIS_EC_REPLICATED,
    AIS_LOCATION,
    AIS_MIRROR_COPIES,
    AIS_MIRROR_PATHS,
    AIS_PRESENT,
    AIS_VERSION,
    HEADER_CONTENT_LENGTH,
    HEADER_ETAG,
    HEADER_LAST_MODIFIED,
)


@dataclass
class ECInfo:
    """Erasure-coding metadata for an object."""

    generation: int = 0
    data_slices: int = 0
    parity_slices: int = 0
    is_ec_copy: bool = False


@dataclass
class ChunksInfo:
    """Chunk metadata for an object."""

    chunk_count: int = 0
    max_chunk_size: int = 0


# pylint: disable=too-few-public-methods
class ObjectAttributes:
    """Information about an object returned by AIS.

    This includes values such as size, checksum, version, presence, and storage
    location. The `Object` stores the object's name, bucket, and provider.
    """

    def __init__(self, response_headers: CaseInsensitiveDict):
        self._response_headers = response_headers

    def _parse_int_header(self, name: str) -> int:
        try:
            return int(self._response_headers.get(name, 0))
        except (TypeError, ValueError):
            return 0

    @property
    def size(self) -> int:
        """Size of the object content."""
        return self._parse_int_header(HEADER_CONTENT_LENGTH)

    @property
    def checksum_type(self) -> str:
        """Checksum type, such as xxhash or md5."""
        return self._response_headers.get(AIS_CHECKSUM_TYPE, "")

    @property
    def checksum_value(self) -> str:
        """Checksum value."""
        return self._response_headers.get(AIS_CHECKSUM_VALUE, "")

    @property
    def access_time(self) -> str:
        """Object access time."""
        return self._response_headers.get(AIS_ACCESS_TIME, "")

    @property
    def obj_version(self) -> str:
        """Object version."""
        return self._response_headers.get(AIS_VERSION, "")

    @property
    def custom_metadata(self) -> Dict[str, str]:
        """Custom object metadata."""
        custom_md_header = self._response_headers.get(AIS_CUSTOM_MD, "")
        return self._parse_custom(custom_md_header) if custom_md_header else {}

    @property
    def present(self) -> bool:
        """Whether the object is present in the cluster."""
        return self._response_headers.get(AIS_PRESENT, "") == "true"

    @property
    def location(self) -> str:
        """Location of the object on its target."""
        return self._response_headers.get(AIS_LOCATION, "")

    @property
    def mirror_paths(self) -> List[str]:
        """Filesystem paths containing mirrored copies of the object."""
        value = self._response_headers.get(AIS_MIRROR_PATHS, "").strip("[]")
        return value.split(",") if value else []

    @property
    def mirror_copies(self) -> int:
        """Number of mirrored copies of the object."""
        return self._parse_int_header(AIS_MIRROR_COPIES)

    @property
    def ec(self) -> Optional[ECInfo]:
        """Erasure-coding metadata, or None when unavailable."""
        headers = (
            AIS_EC_GENERATION,
            AIS_EC_DATA,
            AIS_EC_PARITY,
            AIS_EC_REPLICATED,
        )
        if not any(self._response_headers.get(header, "") for header in headers):
            return None
        return ECInfo(
            generation=self._parse_int_header(AIS_EC_GENERATION),
            data_slices=self._parse_int_header(AIS_EC_DATA),
            parity_slices=self._parse_int_header(AIS_EC_PARITY),
            is_ec_copy=self._response_headers.get(AIS_EC_REPLICATED, "") == "true",
        )

    @property
    def last_modified(self) -> str:
        """Last modification time in RFC 1123 format."""
        return self._response_headers.get(HEADER_LAST_MODIFIED, "")

    @property
    def etag(self) -> str:
        """Entity tag with surrounding quotes removed."""
        return self._response_headers.get(HEADER_ETAG, "").strip('"')

    @property
    def chunks(self) -> Optional[ChunksInfo]:
        """Chunk metadata, or None for a monolithic object."""
        count = self._response_headers.get(AIS_CHUNKS_COUNT, "")
        max_size = self._response_headers.get(AIS_CHUNKS_MAX_CHUNK_SIZE, "")
        if not count and not max_size:
            return None
        return ChunksInfo(
            chunk_count=self._parse_int_header(AIS_CHUNKS_COUNT),
            max_chunk_size=self._parse_int_header(AIS_CHUNKS_MAX_CHUNK_SIZE),
        )

    @staticmethod
    def _parse_custom(custom_md_header: str) -> Dict[str, str]:
        custom_metadata = {}
        for entry in custom_md_header.split(","):
            parts = entry.strip().split("=", 1)
            if len(parts) == 2:
                custom_metadata[parts[0]] = parts[1]
        return custom_metadata
