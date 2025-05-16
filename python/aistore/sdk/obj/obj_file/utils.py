#
# Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
#

from typing import Iterator, Tuple

from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.obj.obj_file.errors import ObjectFileReaderMaxResumeError
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


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
    content_iterator: ContentIterator,
    resume_position: int,
    resume_total: int,
    max_resume: int,
    err: Exception,
) -> Tuple[Iterator[bytes], int]:
    """
    Handle the broken stream/iterator by incrementing the resume count, logging a warning,
    and returning a newly instanatiated iterator from the last known position.

    Args:
        content_iterator (ContentIterator): The content iterator used to read the data.
        resume_position (int): The byte position from which to resume reading.
        resume_total (int): The current number of resume attempts.
        max_resume (int): The maximum number of resume attempts allowed.
        err (Exception): The error that caused the resume attempt.

    Returns:
        Tuple[Iterator[bytes], int]: The new iterator and the updated resume total.

    Raises:
        ObjectFileReaderMaxResumeError: If the maximum number of resume attempts is exceeded.
    """
    resume_total = increment_resume(resume_total, max_resume, err)
    obj_path = content_iterator.client.path
    logger.warning(
        "Error while reading '%s', retrying %d/%d",
        obj_path,
        resume_total,
        max_resume,
        exc_info=err,
    )

    # Create a new iterator from the last read position
    new_iter = content_iterator.iter(offset=resume_position)
    return new_iter, resume_total
