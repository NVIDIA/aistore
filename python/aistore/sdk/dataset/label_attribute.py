#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Callable, Tuple
from aistore.sdk.dataset.config_attribute import ConfigAttribute


# pylint: disable=too-few-public-methods
class LabelAttribute(ConfigAttribute):
    """
    Defines the labeling attributes for a dataset, mapping filenames to corresponding labels

    Args:
        name (str): The name of the attribute, used as a reference and identifier in the dataset
        label_identifier (Callable[[str], str]): A function that maps a file name to a label
    """

    def __init__(self, name: str, label_identifier: Callable[[str], str]):
        self.name = name
        self.label_identifier = label_identifier

    def get_data_for_entry(self, filename: str) -> Tuple[str, any]:
        """
        Get the data for a given filename

        Args:
            filename (str): The name of the file to retrieve data for

        Returns:
            Tuple[str, any]: A tuple containing the sample key and the data for the given filename
        """
        data = self.label_identifier(filename)
        key = self.name
        return key, data
