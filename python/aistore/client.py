"""
This module is a temporary patch to support the torchdata integration for AIStore SDK versions > 1.04.
Torchdata expects the sdk structure to have aistore.client.Client, but Client now exists in aistore.sdk.api
"""

# pylint: disable=unused-variable,unused-import
from aistore.sdk.api import Client
