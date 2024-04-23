#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Tuple
from pathlib import Path
from aistore.sdk.dataset.config_attribute import ConfigAttribute
from aistore.sdk.utils import read_file_bytes


# pylint: disable=too-few-public-methods
class DataAttribute(ConfigAttribute):
    """
    Handles data attributes for datasets, managing the retrieval of attribute data from files

    Args:
        path (Path): The file path where the attribute data is stored
        name (str): The name of the attribute, used as a reference and identifier in the dataset
        file_type (str): The type of file that stores the attribute data (e.g., 'jpg', 'png', 'csv')
    """

    def __init__(self, path: Path, name: str, file_type: str):
        self.path = path
        self.name = name
        self.file_type = file_type

    def get_data_for_entry(self, filename: str) -> Tuple[str, any]:
        """
        Get the data for a given filename

        Args:
            filename (str): The name of the file to retrieve data for

        Returns:
            Tuple[str, any]: A tuple containing the sample key and the data for the given filename
        """
        key = self.name + "." + self.file_type
        try:
            data = read_file_bytes(self.path.joinpath(filename + "." + self.file_type))
        except FileNotFoundError as err:
            print(f"File not found: {err}")
            data = None
        return key, data
