#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#


class MultipartDecodeError(Exception):
    """Raised when multipart decoding fails."""


class BatchTruncatedError(RuntimeError):
    """Raised when archive iteration ends with fewer entries than requested."""
