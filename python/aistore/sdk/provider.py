from __future__ import annotations

from enum import Enum
from typing import Union

from aistore.sdk.errors import InvalidBckProvider

ALIAS_S3 = "s3"
ALIAS_GS = "gs"


class Provider(Enum):
    """
    Represent the providers used for a bucket. See https://aistore.nvidia.com/docs/providers and api/apc/provider.go
    """

    AIS = "ais"
    AMAZON = "aws"
    AZURE = "azure"
    GOOGLE = "gcp"
    HTTP = "ht"

    @staticmethod
    def parse(provider: Union[Provider, str]) -> Provider:
        """
        Parse a provider Enum instance from a given value.
        Args:
            provider: A Provider or string.

        Returns: The given Provider or a new one constructed from the given value.

        Raises: InvalidBckProvider if provided with a string that is not a valid Provider option.
        """
        if isinstance(provider, Provider):
            return provider
        try:
            # Use the provider alias for the given string if one exists
            provider = provider_aliases.get(provider, provider)
            return Provider(provider)
        except ValueError as exc:
            raise InvalidBckProvider(provider) from exc


provider_aliases = {ALIAS_GS: Provider.GOOGLE.value, ALIAS_S3: Provider.AMAZON.value}
