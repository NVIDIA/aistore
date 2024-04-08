#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import List, Dict, Any, Generator
from pathlib import Path
from aistore.sdk.dataset.config_attribute import ConfigAttribute


# pylint: disable=too-few-public-methods
class DatasetConfig:
    """
    Represents the configuration for managing datasets, particularly focusing on how data attributes are structured

    Args:
        primary_attribute (ConfigAttribute): The primary key used for looking up any secondary_attributes will
            be determined by the filename of each sample defined by primary_attribute
        secondary_attributes (List[ConfigAttribute], optional): A list of configurations for
            each attribute or feature in the dataset
    """

    def __init__(
        self,
        primary_attribute: ConfigAttribute,
        secondary_attributes: List[ConfigAttribute] = None,
    ):
        self.primary_attribute = primary_attribute
        self.secondary_attributes = secondary_attributes if secondary_attributes else []

    def generate_dataset(
        self, max_shard_items: int
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Generate a dataset in webdataset format

        Args:
            max_shard_items (int): The maximum number of items to include in a shard

        Returns:
            Generator[Dict[str, Any]]: A generator that yields samples in webdataset format
        """
        all_attributes = [self.primary_attribute] + self.secondary_attributes
        # Generate the dataset
        for index, file in enumerate(
            Path(self.primary_attribute.path).glob(
                "*." + self.primary_attribute.file_type
            )
        ):
            filename = file.stem
            item = {}
            for cfg in all_attributes:
                key, data = cfg.get_data_for_entry(filename)
                if not data:
                    item = None
                    break
                item[key] = data
            if not item:
                continue
            item["__key__"] = self._get_format_string(max_shard_items) % index
            yield item

    @staticmethod
    def _get_format_string(val) -> str:
        """
        Get a __key__ string for an item in webdataset format
        """
        num_digits = len(str(val))
        format_str = "sample_%0" + str(num_digits) + "d"
        return format_str
