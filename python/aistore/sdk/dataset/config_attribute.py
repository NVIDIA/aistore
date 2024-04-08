#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from abc import ABC, abstractmethod
from typing import Tuple


# pylint: disable=too-few-public-methods
class ConfigAttribute(ABC):
    """
    Abstract class for defining the configuration of an attribute in a dataset
    """

    @abstractmethod
    def get_data_for_entry(self, filename: str) -> Tuple[str, any]:
        """
        Get the data for a given filename

        Args:
            filename (str): The name of the file to retrieve data for

        Returns:
            Tuple[str, any]: A tuple containing the sample key and the data for the given filename
        """
