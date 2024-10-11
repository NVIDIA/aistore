#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from io import BufferedIOBase

from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.obj.obj_file.buffer import SimpleBuffer
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class ObjectFile(BufferedIOBase):
    """
    A file-like object extending `BufferedIOBase` for reading object data, with support for both
    reading a fixed size of data and reading until the end of file (EOF).

    Data is retrieved from the provided `content_iterator` in chunks and read through an internal
    buffer. The buffer manages retries and error recovery (e.g., ChunkedEncodingError) as needed.
    The `max_resume` setting applies to the entire ObjectFile instance, specifying the maximum number
    of retry attempts allowed to recover from unexpected stream interruptions during reads.

    Args:
        content_iterator (ContentIterator): An iterator that can fetch object data from AIS in chunks.
        max_resume (int): Maximum number of retry attempts in case of a streaming failure.
    """

    def __init__(self, content_iterator: ContentIterator, max_resume: int):
        self._content_iterator = content_iterator
        self._max_resume = max_resume
        self._read_position = 0
        self._closed = False
        self._buffer = SimpleBuffer(self._content_iterator, self._max_resume)

    def __enter__(self):
        logger.debug("Entering context, resetting file state.")
        self._closed = False
        self._read_position = 0
        self._buffer = SimpleBuffer(self._content_iterator, self._max_resume)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        logger.debug("Exiting context, closing file.")
        self.close()

    def close(self) -> None:
        """
        Close the file and release resources.

        Raises:
            ValueError: I/O operation on closed file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        logger.debug("Closing file.")
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
        return self._read_position

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

        Raises:
            ObjectFileStreamError if a connection cannot be made.
            ObjectFileMaxResumeError if the stream is interrupted more than the allowed maximum.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if size == 0:
            return b""

        # Fill buffer to the required size
        try:
            self._buffer.fill(size)
        except Exception as err:
            logger.error("Error filling buffer, closing file: (%s)", err)
            self.close()
            raise err

        # Read data from the filled buffer
        data = self._buffer.read(size)
        self._read_position += len(data)
        return data
