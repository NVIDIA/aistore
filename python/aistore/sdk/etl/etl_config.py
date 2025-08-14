#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass
from typing import Dict, Optional, Union
from json import dumps as json_dumps
from aistore.sdk.const import QPARAM_ETL_NAME, QPARAM_ETL_ARGS, QPARAM_ETL_PIPELINE
from aistore.sdk.etl.etl import Etl


@dataclass
class ETLConfig:
    """
    Configuration for ETL operations.

    Attributes:
        name (Optional[Union[str, Etl]]): Name of the ETL pipeline or an Etl object representing a pipeline.
        args (Optional[Union[str, Dict]]): Optional parameters for configuring the ETL pipeline.
            Can be provided as:
                - A string for simple arguments.
                - A dictionary for structured key-value configurations.

    Example:
        etl_config = ETLConfig(
            name="image-transform",
            args={"format": "jpeg", "resize": "256x256"}
        )
    """

    name: Optional[Union[str, Etl]] = None
    args: Optional[Union[str, Dict]] = None

    def update_qparams(self, params: Dict[str, str]) -> None:
        """
        Apply ETL configuration to query parameters.

        Args:
            params (Dict[str, str]): Query parameters dict to modify
        """
        if not self.name:
            return

        if isinstance(self.name, Etl):
            params[QPARAM_ETL_NAME] = self.name.name
            params[QPARAM_ETL_PIPELINE] = ",".join(self.name.pipeline)
        else:
            params[QPARAM_ETL_NAME] = self.name

        if self.args:
            etl_args = (
                json_dumps(self.args, separators=(",", ":"))
                if isinstance(self.args, dict)
                else self.args
            )
            params[QPARAM_ETL_ARGS] = etl_args
