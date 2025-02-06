#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass
from typing import Dict, Optional, Union


@dataclass
class ETLConfig:
    """
    Configuration for ETL operations.

    Attributes:
        name (str): Name of the ETL pipeline.
        args (Optional[Union[str, Dict]]): Optional parameters for configuring the ETL pipeline.
            Can be provided as:
                - A string for simple arguments.
                - A dictionary for structured key-value configurations.
            Defaults to an empty dictionary.

    Example:
        etl_config = ETLConfig(
            name="image-transform",
            args={"format": "jpeg", "resize": "256x256"}
        )
    """

    name: str
    args: Optional[Union[str, Dict]] = None
