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

from aistore.sdk.obj.content_iterator import BaseContentIterProvider, StreamBounds
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
        self._bounds = StreamBounds()
        self._delivered_position = 0
        self._stream_consumed = 0
        self._resumes = 0
        self.restart()

    @property
    def path(self) -> str:
        """Path of the object being read."""
        return self._content_provider.client.path

    @property
    def delivered_position(self) -> int:
        """
        Position of the cursor as seen by the caller.
        Equivalent to the number of bytes delivered so far.
        """
        return self._delivered_position

    @property
    def _stream_position(self) -> int:
        """
        How far the current stream has read. This trails `delivered_position` while a
        stream replays bytes the caller already holds, and the two match otherwise.
        Valid only after the current stream yields or ends.
        """
        return self._bounds.start + self._stream_consumed

    @property
    def resumes(self) -> int:
        """Number of resumes performed so far."""
        return self._resumes

    def restart(self) -> None:
        """Discard all progress and open a fresh stream at the start of the object."""
        self._open()
        self._delivered_position = 0
        self._resumes = 0

    def _open(self, offset: int = 0) -> None:
        self.close()
        # One provider can back several streams, so each stream tracks its own bounds.
        self._bounds = StreamBounds()
        self._content_iter = self._content_provider.create_iter(
            offset=offset, bounds=self._bounds
        )
        self._stream_consumed = 0

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
                # A clean EOF is not proof of completion, so resume if the stream ends before
                # the object does.
                short = self._short_read()
                if not short:
                    raise
                self._resume(ObjectFileReaderUnexpectedEOF(*short))
                continue

            stream_position = self._stream_position
            self._stream_consumed += len(chunk)

            # Discard what the caller already holds.
            replayed = self._delivered_position - stream_position
            if replayed > 0:
                chunk = chunk[replayed:]
                if not chunk:
                    continue

            self._delivered_position += len(chunk)
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

        resumable = self._content_provider.client.can_get_at_offset()
        self._open(self._delivered_position if resumable else 0)

    def _short_read(self) -> Optional[Tuple[int, int]]:
        stream_position = self._stream_position
        expected_end_position = self._bounds.expected_end
        if stream_position < self._delivered_position:
            # Even without a Content-Length, a restarted stream must
            # reach the position already read from the previous stream.
            expected_end_position = max(
                expected_end_position or 0, self._delivered_position
            )

        if expected_end_position is None or stream_position >= expected_end_position:
            return None
        return stream_position, expected_end_position
