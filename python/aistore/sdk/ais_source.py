#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#
from abc import ABC, abstractmethod
from typing import Iterable


# pylint: disable=too-few-public-methods
class AISSource(ABC):
    """
    Interface for all AIS class types providing access to AIS objects via URLs
    """

    @abstractmethod
    def list_urls(self, prefix: str = "", etl_name: str = None) -> Iterable[str]:
        """
        Get an iterable of urls to reference the objects contained in this source (bucket, group, etc.)
        Args:
            prefix (str, optional): Only include objects with names matching this prefix
            etl_name (str, optional): Apply an ETL when retrieving object contents

        Returns:
            Iterable over selected object URLS
        """
