#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from sys import maxsize
from typing import Iterator, Tuple, Optional

from aistore.sdk.obj.content_iter_provider import ContentIterProvider
from aistore.sdk.obj.obj_file.errors import ObjectFileReaderMaxResumeError
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


def compute_loop_size(size: int) -> int:
    """
    Compute the size of the loop for reading data and return it.
    If requested read size is -1, return `sys.maxsize` to loop until `StopIteration`.

    Args:
        size (int): The requested size to be read.

    Returns:
        int: The size for the loop.

    """
    return maxsize if size == -1 else size


def get_iterator(
    content_provider: ContentIterProvider, resume_position: int
) -> Optional[Iterator[bytes]]:
    """
    Create a new iterator from the content iterator starting at the specified byte position.
    Returns None if the object is not cached.

    Args:
        content_provider (ContentIterProvider): The content iterator used to read the data.
        resume_position (int): The byte position from which to resume reading.

    Returns:
        Optional[Iterator[bytes]]: A new iterator starting from the specified byte position.
            None if the object is not cached in the bucket.
    """
    # If remote object is not cached, start over
    if not content_provider.client.head().present:
        return None
    # Otherwise, resume from last known position
    else:
        return content_provider.create_iter(offset=resume_position)


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
        ObjectFileReaderMaxResumeError: If the number of resume attempts exceeds the maximum allowed.
    """
    resume_total += 1
    if resume_total > max_resume:
        raise ObjectFileReaderMaxResumeError(err, resume_total) from err
    return resume_total


def handle_broken_stream(
    content_provider: ContentIterProvider,
    resume_position: int,
    resume_total: int,
    max_resume: int,
    err: Exception,
) -> Tuple[Optional[Iterator[bytes]], int]:
    """
    Handle the broken stream/iterator by incrementing the resume count, logging a warning,
    and returning a newly instanatiated iterator from the last known position.

    Args:
        content_provider (ContentIterProvider): The content iterator used to read the data.
        resume_position (int): The byte position from which to resume reading.
        resume_total (int): The current number of resume attempts.
        max_resume (int): The maximum number of resume attempts allowed.
        err (Exception): The error that caused the resume attempt.

    Returns:
        Optional[Iterator[bytes]]: The new iterator. None if the object is not cached.
        int: The updated number of resume attempts.

    Raises:
        ObjectFileReaderMaxResumeError: If the maximum number of resume attempts is exceeded.
    """
    resume_total = increment_resume(resume_total, max_resume, err)
    obj_path = content_provider.client.path
    logger.warning(
        "Error while reading '%s', retrying %d/%d",
        obj_path,
        resume_total,
        max_resume,
        exc_info=err,
    )

    new_iter = get_iterator(
        content_provider=content_provider, resume_position=resume_position
    )

    return new_iter, resume_total
