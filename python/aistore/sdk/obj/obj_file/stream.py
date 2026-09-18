#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

from typing import Generator, Optional, Tuple

from requests.exceptions import (
    ChunkedEncodingError,
    ConnectionError as RequestsConnectionError,
    ReadTimeout,
)
from urllib3.exceptions import ProtocolError, ReadTimeoutError

from aistore.sdk.obj.content_iterator import BaseContentIterProvider
from aistore.sdk.obj.obj_file.errors import (
    ObjectFileReaderMaxResumeError,
    ObjectFileReaderUnexpectedEOF,
)
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)

STREAM_ERRORS = (
    RequestsConnectionError,
    ChunkedEncodingError,
    ProtocolError,
    ReadTimeout,
    ReadTimeoutError,
)


class ResumableStream:
    """
    Delivers an object's bytes in chunks, replacing the underlying stream whenever it
    fails or ends before the object does.

    Args:
        content_provider (BaseContentIterProvider): A provider that creates iterators which
            can fetch object data from AIS in chunks.
        max_resume (int): Maximum number of resumes allowed for a single pass over the object.
    """

    def __init__(self, content_provider: BaseContentIterProvider, max_resume: int):
        self._content_provider = content_provider
        self._max_resume = max_resume
        # Declared here so static analysis sees them as instance attributes;
        # actual values are (re)assigned by restart().
        self._content_iter: Optional[Generator[bytes, None, None]] = None
        self._position = 0
        self._bytes_to_skip = 0
        self._resumes = 0
        self.restart()

    @property
    def path(self) -> str:
        """Path of the object being read."""
        return self._content_provider.client.path

    @property
    def position(self) -> int:
        """Number of bytes delivered so far."""
        return self._position

    @property
    def resumes(self) -> int:
        """Number of resumes performed so far."""
        return self._resumes

    def restart(self) -> None:
        """Discard all progress and open a fresh stream at the start of the object."""
        self._content_iter = self._content_provider.create_iter()
        self._position = 0
        self._bytes_to_skip = 0
        self._resumes = 0

    def close(self) -> None:
        """Release the underlying stream."""
        if self._content_iter:
            self._content_iter.close()

    def __iter__(self) -> "ResumableStream":
        return self

    def __next__(self) -> memoryview:
        """
        Return the next chunk of object data.

        Raises:
            StopIteration: If the object has been delivered in full.
            ObjectFileReaderMaxResumeError: If the maximum number of resume attempts is exceeded.
        """
        while True:
            try:
                chunk = memoryview(next(self._content_iter))
            except STREAM_ERRORS as err:
                # The stream broke (e.g. TCP reset, dropped connection, malformed or
                # incomplete chunk) or timed out.
                self._resume(err)
                continue
            except StopIteration:
                short = self._short_read()
                if not short:
                    raise
                self._resume(ObjectFileReaderUnexpectedEOF(*short))
                continue

            # A restarted stream replays bytes already delivered to the caller.
            if self._bytes_to_skip:
                skipped = min(self._bytes_to_skip, len(chunk))
                self._bytes_to_skip -= skipped
                chunk = chunk[skipped:]
                if not chunk:
                    continue

            self._position += len(chunk)
            return chunk

    def _resume(self, err: Exception) -> None:
        self._resumes += 1
        if self._resumes > self._max_resume:
            raise ObjectFileReaderMaxResumeError(err, self._resumes) from err

        logger.warning(
            "Resuming '%s' after %s (%d/%d)",
            self.path,
            err,
            self._resumes,
            self._max_resume,
        )

        # A remote object that is not cached must start over. Required even on clean short
        # EOF: under Streaming-Cold-GET the object may not be fully cached yet, and a range
        # resume can hang.
        if not self._content_provider.client.head().present:
            self._bytes_to_skip = self._position
            self._content_iter = self._content_provider.create_iter()
            return

        self._bytes_to_skip = 0
        self._content_iter = self._content_provider.create_iter(offset=self._position)

    def _short_read(self) -> Optional[Tuple[int, int]]:
        expected_end_position = self._expected_end_position()
        if self._bytes_to_skip:
            # Even without a Content-Length, a restarted stream must
            # reach the position already read from the previous stream.
            expected_end_position = max(expected_end_position or 0, self._position)

        stream_position = self._position - self._bytes_to_skip
        if expected_end_position is None or stream_position >= expected_end_position:
            return None
        return stream_position, expected_end_position

    def _expected_end_position(self) -> Optional[int]:
        # Treat anything that isn't a real int as unknown.
        expected = self._content_provider.expected_end_position
        return expected if isinstance(expected, int) else None
