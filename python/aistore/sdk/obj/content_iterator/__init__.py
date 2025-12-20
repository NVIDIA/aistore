#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

"""
Content iterator providers for streaming object data.

This package provides iterators for fetching object content either sequentially
or in parallel using concurrent range-reads.
"""

from aistore.sdk.obj.content_iterator.base import BaseContentIterProvider
from aistore.sdk.obj.content_iterator.sequential import ContentIterProvider
from aistore.sdk.obj.content_iterator.parallel import ParallelContentIterProvider

__all__ = [
    "BaseContentIterProvider",
    "ContentIterProvider",
    "ParallelContentIterProvider",
]
