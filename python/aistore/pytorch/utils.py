"""
Utils for AIS PyTorch Plugin

Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

from urllib.parse import urlunparse
from typing import Tuple
from aistore.sdk.utils import parse_url as sdk_parse_url


def unparse_url(provider: str, bck_name: str, obj_name: str) -> str:
    """
    Generate URL based on provider, bucket name, and object name.

    Args:
        provider (str): Provider name ('ais', 'gcp', etc.)
        bck_name (str): Bucket name
        obj_name (str): Object name with extension

    Returns:
        str: Complete URL
    """
    return urlunparse([provider, bck_name, obj_name, "", "", ""])


def get_basename(name: str) -> str:
    """
    Get the basename of the object name by stripping any directory information and suffix.

    Args:
        name (str): Complete object name

    Returns:
        str: Basename of the object
    """

    return name.split("/")[-1].split(".")[0]


def parse_url(url: str) -> Tuple[str, str, str]:
    """
    Wrapper of sdk/utils.py parse_url. Parse AIS URLs for bucket and object names.
    TODO: This can be removed once the upstream torch package for aiso is updated.

    Args:
        url (str): Complete URL of the object (e.g., "ais://bucket1/file.txt")

    Returns:
        Tuple[str, str, str]: Provider, bucket name, and object name
    """
    return sdk_parse_url(url)
