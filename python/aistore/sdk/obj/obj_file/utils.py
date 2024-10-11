#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from typing import Iterator, Optional

from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.utils import get_logger
from aistore.sdk.obj.obj_file.errors import (
    ObjectFileStreamError,
    ObjectFileMaxResumeError,
)

logger = get_logger(__name__)


def reset_iterator(
    content_iterator: ContentIterator,
    resume_position: Optional[int] = 0,
) -> Iterator[bytes]:
    """
    Return a new iterator for establishing an object stream and reading chunks of data from
    byte position `resume_position`.

    Args:
        content_iterator (ContentIterator): An instance of `ContentIterator` to read data from.
        resume_position (int, optional): The byte position to resume reading from. Defaults to 0.

    Returns:
        Iterator[bytes]: An iterator to read chunks of data from the object stream.

    Raises:
        ObjectFileStreamError if a connection cannot be made.
    """
    try:
        return content_iterator.iter_from_position(resume_position)
    except Exception as err:
        logger.error("Error establishing object stream: (%s)", err)
        raise ObjectFileStreamError(err) from err


def increment_resume(resume_total: int, max_resume: int, err: Exception) -> int:
    """
    Increment the number of resume attempts and raise an error if the maximum allowed is exceeded.

    Args:
        resume_total (int): The number of resume attempts made.
        max_resume (int): The maximum number of resume attempts allowed.
        err (Exception): The error that triggered the resume attempt.

    Returns:
        int: The updated number of resume attempts.

    Raises:
        ObjectFileMaxResumeError: If the number of resume attempts exceeds the maximum allowed.
    """
    resume_total += 1
    if resume_total > max_resume:
        raise ObjectFileMaxResumeError(err, resume_total) from err
    return resume_total
