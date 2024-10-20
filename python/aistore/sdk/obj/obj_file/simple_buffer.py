#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

import requests
from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.obj.obj_file.utils import handle_chunked_encoding_error, reset_iterator
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class SimpleBuffer:
    """
    A buffer for handling chunked streamed data with the ability to resume from the last known position.
    """

    def __init__(
        self, content_iterator: ContentIterator, max_resume: int, buffer_size: int
    ):
        self._content_iterator = content_iterator
        self._resume_position = 0
        self._chunk_iterator = reset_iterator(
            self._content_iterator, self._resume_position
        )
        self._max_resume = max_resume
        self._resume_total = 0
        self._buffer = bytearray()
        self._buffer_size = buffer_size
        self._eof = False

    @property
    def eof(self) -> bool:
        """The end-of-file status of the content iterator."""
        return self._eof

    def __len__(self) -> int:
        return len(self._buffer)

    def read(self, size: int = -1) -> bytes:
        """
        Return the requested bytes read from the buffer. If the buffer does not contain enough data,
        or the requested size is -1, return all remaining bytes.

        The buffer is trimmed after reading the data.

        Args:
            size (int, optional): Number of bytes to read from the buffer. If -1, reads all
                                  remaining bytes.

        Returns:
            bytes: The data read from the buffer.
        """
        if size == -1 or size >= len(self._buffer):
            size = len(self._buffer)
        retval = memoryview(self._buffer)[:size]
        self._buffer = self._buffer[size:]
        return bytes(retval)

    def fill(self) -> None:
        """
        Fill the buffer with chunks up to the buffer size or until the stream is exhausted.

        Raises:
            ObjectFileStreamError if a connection cannot be made.
            ObjectFileMaxResumeError if the stream is interrupted more than the allowed maximum.
        """
        while True:
            try:
                # Keep filling the buffer until it reaches the buffer size or stream ends
                while len(self._buffer) < self._buffer_size:
                    chunk = next(self._chunk_iterator)
                    self._buffer.extend(chunk)
                    self._resume_position += len(chunk)
                return
            except StopIteration:
                # No more data in the stream (EOF)
                self._eof = True
                return
            except requests.exceptions.ChunkedEncodingError as err:
                self._chunk_iterator, self._resume_total = (
                    handle_chunked_encoding_error(
                        self._content_iterator,
                        self._resume_position,
                        self._resume_total,
                        self._max_resume,
                        err,
                    )
                )
