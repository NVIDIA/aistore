#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import logging
from typing import List, Dict, Any, Generator, Tuple
from pathlib import Path

from aistore.sdk.dataset.config_attribute import ConfigAttribute
from aistore.sdk.const import DEFAULT_DATASET_MAX_COUNT


# pylint: disable=too-few-public-methods,import-outside-toplevel
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

    def write_shards(self, skip_missing: bool, **kwargs):
        """
        Write the dataset to a bucket in webdataset format and log the missing attributes

        Args:
            skip_missing (bool, optional): Skip samples that are missing one or more attributes, defaults to True
            **kwargs: Additional arguments to pass to the webdataset writer
        """
        # Avoid module-level import
        try:
            from webdataset import (
                ShardWriter,
            )
        except ImportError as e:
            raise ImportError(
                "The 'webdataset' module is required to use 'write_shards'."
                "Please install it with 'pip install webdataset'."
            ) from e
        logger = logging.getLogger(f"{__name__}.write_shards")
        max_shard_items = kwargs.get("maxcount", DEFAULT_DATASET_MAX_COUNT)
        num_digits = len(str(max_shard_items))
        kwargs["pattern"] = kwargs.get("pattern", "dataset") + f"-%0{num_digits}d.tar"
        shard_writer = ShardWriter(**kwargs)

        dataset = self.generate_dataset(max_shard_items)
        for sample, missing_attributes in dataset:
            if missing_attributes:
                missing_attributes_str = ", ".join(missing_attributes)
                if skip_missing:
                    logger.warning(
                        "Missing attributes: %s - Skipping sample.",
                        missing_attributes_str,
                    )
                    continue
                logger.warning(
                    "Missing attributes: %s - Including sample despite missing attributes.",
                    missing_attributes_str,
                )
            shard_writer.write(sample)

        shard_writer.close()

    def generate_dataset(
        self,
        max_shard_items: int,
    ) -> Generator[Tuple[Dict[str, Any], List[str]], None, None]:
        """
        Generate a dataset in webdataset format

        Args:
            max_shard_items (int): The maximum number of items to include in a shard

        Returns:
            Generator (Tuple[Dict[str, Any], List[str]]): A generator that yields samples in webdataset format
                and a list of missing attributes
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
            missing_attributes = []
            for cfg in all_attributes:
                key, data = cfg.get_data_for_entry(filename)
                if not data:
                    missing_attributes.append(f"{filename} - {key}")
                else:
                    item[key] = data
            item["__key__"] = self._get_format_string(max_shard_items) % index
            yield item, missing_attributes

    @staticmethod
    def _get_format_string(val) -> str:
        """
        Get a __key__ string for an item in webdataset format
        """
        num_digits = len(str(val))
        format_str = "sample_%0" + str(num_digits) + "d"
        return format_str
