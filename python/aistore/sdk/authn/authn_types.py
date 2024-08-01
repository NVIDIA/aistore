#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations
from typing import Optional, Union
from pydantic import BaseModel, Field
from aistore.sdk.const import NANOSECONDS_IN_SECOND


class LoginMsg(BaseModel):
    """
    Represents a login message with a password and optional expiration duration.

    Attributes:
        password (str): The password string.
        expires_in (Optional[Union[int, float]]): The expiration duration in seconds.
    """

    password: str
    expires_in: Optional[Union[int, float]] = Field(
        None, description="Expiration duration in seconds."
    )

    def as_dict(self):
        """
        Converts the instance to a dict, converting the expiration duration to nanoseconds if specified.

        Returns:
            Dict[str, Union[str, int]]: The dict representation of the login message.
        """

        data = self.dict()
        if self.expires_in is not None:
            data["expires_in"] = int(
                self.expires_in * NANOSECONDS_IN_SECOND
            )  # Convert seconds to nanoseconds
        return data


class TokenMsg(BaseModel):
    """
    Represents a message containing a token.

    Attributes:
        token (str): The token string.
    """

    token: str
