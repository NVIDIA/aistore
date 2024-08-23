#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=too-many-branches

import logging
from io import BufferedIOBase
from typing import Iterator

import requests

from aistore.sdk.object_reader import ObjectReader

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleBuffer:
    """
    A buffer for efficiently handling streamed data with position tracking.

    It stores incoming chunks of data in a bytearray and tracks the current read position.
    Once data is read, it is discarded from the buffer to free memory, ensuring efficient
    usage.
    """

    def __init__(self):
        self._buffer = bytearray()
        self._pos = 0

    def __len__(self):
        """
        Return the number of unread bytes in the buffer.

        Returns:
            int: The number of unread bytes remaining in the buffer.
        """
        return len(self._buffer) - self._pos

    def read(self, size: int = -1) -> bytes:
        """
        Read bytes from the buffer and advance the read position.

        Args:
            size (int, optional): Number of bytes to read from the buffer. If -1, reads all
                remaining bytes.

        Returns:
            bytes: The data read from the buffer.
        """
        if size < 0 or size > len(self):
            size = len(self)

        data = self._buffer[self._pos : self._pos + size]
        self._pos += len(data)
        return bytes(data)

    def fill(self, source: Iterator[bytes], size: int = -1):
        """
        Fill the buffer with data from the source, up to the specified size.
        Args:
            source (Iterator[bytes]): The data source (chunks).
            size (int, optional): The target size to fill the buffer up to. Default is -1 for unlimited.
        Returns:
            int: Number of bytes in the buffer.
        """
        if self._pos != 0:
            # Remove already-read data
            self._buffer = self._buffer[self._pos :]
            self._pos = 0

        while len(self._buffer) < size or size < 0:
            try:
                chunk = next(source)
                self._buffer.extend(chunk)
            except StopIteration:
                break

        return len(self)

    def empty(self):
        """Empty the buffer."""
        self._buffer = bytearray()
        self._pos = 0


class ObjectFile(BufferedIOBase):
    """
    A file-like object for reading object data, with support for both reading a fixed size
    of data and reading until the end of the stream (EOF). It provides the ability to resume
    and continue reading from the last known position in the event of a streaming error, such
    as a `ChunkedEncodingError`.

    Data is fetched in chunks via the object reader and temporarily stored in an internal buffer.
    The buffer is filled either to the required size or until EOF is reached. If a streaming
    error occurs during this process, the object file automatically attempts to resume the buffer
    filling process from the last known chunk position, up to a configurable number of resume
    attempts (`max_resume`), raising a `ChunkedEncodingError` if the maximum number of attempts
    is exceeded.

    Once the buffer is adequately filled, the `read()` method reads and returns the requested amount
    of data from the buffer.

    Args:
        object_reader (ObjectReader): The object reader used to fetch object data in chunks.
        max_resume (int): Maximum number of retry attempts in case of a streaming failure.
    """

    def __init__(self, object_reader: ObjectReader, max_resume: int):
        self._object_reader = object_reader
        self._max_resume = max_resume
        self._current_pos = 0
        self._closed = False
        self._buffer = SimpleBuffer()
        self._chunk_iterator = self._object_reader.iter_from_position(self._current_pos)

    def close(self) -> None:
        """
        Close the file and release resources.

        Raises:
            ValueError: I/O operation on closed file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        logger.debug("Closing file.")
        self._buffer.empty()
        self._chunk_iterator = None
        self._closed = True

    def tell(self) -> int:
        """
        Return the current file position.

        Returns:
            The current file position.

        Raises:
            ValueError: I/O operation on closed file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        return self._current_pos

    def readable(self) -> bool:
        """
        Return whether the file is readable.

        Returns:
            True if the file is readable, False otherwise.

        Raises:
            ValueError: I/O operation on closed file.
        """
        if self._closed:
            return False
        return True

    def seekable(self) -> bool:
        """
        Return whether the file supports seeking.

        Returns:
            False since the file does not support seeking.
        """
        return False

    def read(self, size=-1):
        """
        Read bytes from the object, handling retries in case of stream errors.

        Args:
            size (int, optional): Number of bytes to read. If -1, reads until the end of the stream.

        Returns:
            bytes: The data read from the object.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if size == 0:
            return b""

        resume_attempts = 0

        while True:
            try:
                # Fill the buffer with the requested size or to EOF
                self._buffer.fill(self._chunk_iterator, size)
                # If successfully filled the buffer, exit
                break
            except requests.exceptions.ChunkedEncodingError as err:
                resume_attempts += 1
                if resume_attempts > self._max_resume:
                    logger.error("Max retries reached. Cannot resume read.")
                    raise err
                logger.warning(
                    "Chunked encoding error (%s), retrying %d/%d",
                    err,
                    resume_attempts,
                    self._max_resume,
                )

                # Reset the chunk iterator for resuming the stream
                self._chunk_iterator = self._object_reader.iter_from_position(
                    len(self._buffer)
                )

        # Read data from the buffer
        data = self._buffer.read(size)
        self._current_pos += len(data)
        return data
