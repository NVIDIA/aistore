#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Dict
from requests.structures import CaseInsensitiveDict
from aistore.sdk.const import (
    HEADER_CONTENT_LENGTH,
    AIS_CHECKSUM_TYPE,
    AIS_CHECKSUM_VALUE,
    AIS_ACCESS_TIME,
    AIS_VERSION,
    AIS_CUSTOM_MD,
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
