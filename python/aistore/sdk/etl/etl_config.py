#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass
from typing import Dict, Optional, Union
from json import dumps as json_dumps
from aistore.sdk.const import QPARAM_ETL_NAME, QPARAM_ETL_ARGS


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

    def update_qparams(self, params: Dict[str, str]) -> None:
        """
        Apply ETL configuration to query parameters.

        Args:
            params (Dict[str, str]): Query parameters dict to modify
        """
        params[QPARAM_ETL_NAME] = self.name
        if self.args:
            etl_args = (
                json_dumps(self.args, separators=(",", ":"))
                if isinstance(self.args, dict)
                else self.args
            )
            params[QPARAM_ETL_ARGS] = etl_args
