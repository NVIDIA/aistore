#
# Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
#

from io import BufferedIOBase
from sys import maxsize as sys_maxsize
from typing import Optional
from warnings import warn

from overrides import override

from aistore.sdk.obj.content_iterator import BaseContentIterProvider
from aistore.sdk.obj.obj_file.stream import ResumableStream
from aistore.sdk.utils import get_logger

logger = get_logger(__name__)


class ObjectFileReader(BufferedIOBase):
    """
    A sequential read-only file-like object extending `BufferedIOBase` for reading object data, with support for both
    reading a fixed size of data and reading until the end of file (EOF).

    When a read is requested, any remaining data from a previously fetched chunk is returned first. If the remaining
    data is insufficient to satisfy the request, the `read()` method fetches additional chunks from the provided
    iterator as needed, until the requested size is fulfilled or the end of the stream is reached.

    In case of unexpected stream interruptions (e.g. `ChunkedEncodingError`, `ConnectionError`) or timeouts (e.g.
    `ReadTimeout`), the `read()` method automatically retries and resumes fetching data from the last successfully
    retrieved chunk. The `max_resume` parameter limits consecutive retry attempts without forward progress.
    Total retry count is unlimited as long as reads continue to fetch new bytes.

    Entering a context restarts the reader from the beginning, even after `close()`.

    Args:
        content_provider (BaseContentIterProvider): A provider that creates iterators which
            can fetch object data from AIS in chunks.
        max_resume (int): Maximum consecutive retry attempts without forward progress.
    """

    def __init__(self, content_provider: BaseContentIterProvider, max_resume: int):
        self._stream = ResumableStream(content_provider, max_resume)
        self._remainder: Optional[memoryview] = None
        self._closed = False

    def _reset(self) -> None:
        self._stream.restart()
        self._remainder = None
        self._closed = False

    @override
    def __enter__(self):
        self._reset()
        return self

    @property
    def closed(self) -> bool:
        """Return whether the file is closed."""
        return self._closed

    @override
    def readable(self) -> bool:
        """Return whether the file is readable."""
        return not self._closed

    @override
    def read(self, size: Optional[int] = -1) -> bytes:
        """
        Read up to 'size' bytes from the object. If size is -1, read until the end of the stream.

        Args:
            size (int, optional): The number of bytes to read. If -1, reads until EOF.

        Returns:
            bytes: The read data as a bytes object.

        Raises:
            ObjectFileReaderStreamError: If a connection cannot be made.
            ObjectFileReaderMaxResumeError: If the stream is interrupted more than the allowed maximum.
            ValueError: I/O operation on a closed file.
            Exception: Any other errors while streaming and reading.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")
        if size == 0:
            return b""
        if size is None or size < 0:
            size = sys_maxsize

        try:
            chunk = self._remainder
            if not chunk:
                chunk = next(self._stream, None)
                if chunk is None:
                    return b""

            chunk_size = len(chunk)
            if size < chunk_size:
                self._remainder = chunk[size:]
                return bytes(chunk[:size])

            self._remainder = None
            if size == chunk_size:
                # A full view of plain bytes needs no additional copy.
                data = chunk.obj
                if (
                    type(data) is bytes  # pylint: disable=unidiomatic-typecheck
                    and chunk_size == len(data)
                    and chunk.c_contiguous
                ):
                    return data
                return bytes(chunk)

            size -= chunk_size
            result = self._read_chunks(chunk, size)

        except Exception as err:
            logger.error(
                "Error while reading object at '%s': %s. Closing file.",
                self._stream.path,
                err,
                exc_info=True,
            )
            self.close()
            raise err

        return b"".join(result)

    def _read_chunks(self, first_chunk: memoryview, size: int) -> list[memoryview]:
        """Collect the first chunk and up to size additional bytes, buffering any excess."""
        result = [first_chunk]
        while size:
            chunk = next(self._stream, None)
            if chunk is None:
                break

            chunk_size = len(chunk)
            if size < chunk_size:
                result.append(chunk[:size])
                self._remainder = chunk[size:]
                break
            result.append(chunk)
            size -= chunk_size
        return result

    @override
    def close(self) -> None:
        """Close the file."""
        self._closed = True
        self._stream.close()


class ObjectFileWriter(BufferedIOBase):
    """
    A file-like writer object for AIStore, extending `BufferedIOBase`.

    Writes go directly to AIStore; no local write buffer is added. Use a context
    manager or call `close()` to finalize the object. Finalization only warns
    if the writer is left open; it does not send requests to the cluster.

    Write mode truncates the object when the writer is created. Entering a
    context preserves any data already written by the open writer. Entering a
    context with a closed writer raises `ValueError`. Create a new writer with
    `ObjectWriter.as_file()` to write again.

    Args:
        obj_writer (ObjectWriter): The ObjectWriter instance for handling write operations.
        mode (str): Specifies the mode in which the file is opened.
            - `'w'`: Write mode. Opens the object for writing, truncating any existing content.
                     Writing starts from the beginning of the object.
            - `'a'`: Append mode. Opens the object for appending. Existing content is preserved,
                     and writing starts from the end of the object.
    """

    def __init__(self, obj_writer: "ObjectWriter", mode: str):
        self._obj_writer = obj_writer
        self._mode = mode
        self._handle = ""
        self._closed = True
        if self._mode == "w":
            self._obj_writer.put_content(b"")
        self._closed = False

    @property
    def closed(self) -> bool:
        """Return whether the file is closed."""
        return self._closed

    def __del__(self) -> None:
        # The inherited finalizer calls close(), which can send a remote flush.
        if not getattr(self, "_closed", True):
            warn(f"unclosed {self!r}", ResourceWarning, source=self)

    @override
    def writable(self) -> bool:
        """Return whether the file is writable."""
        return not self.closed

    @override(check_signature=False)  # Preserve the public buffer keyword.
    def write(self, buffer: bytes) -> int:
        """
        Write data to the object.

        Args:
            buffer (bytes): The data to write.

        Returns:
            int: Number of bytes written.

        Raises:
            ValueError: I/O operation on a closed file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        self._handle = self._obj_writer.append_content(buffer, handle=self._handle)

        return len(buffer)

    @override
    def flush(self) -> None:
        """
        Flush the writer, ensuring the object is finalized.

        This does not close the writer but makes the current state accessible.

        Raises:
            ValueError: I/O operation on a closed file.
        """
        if self._closed:
            raise ValueError("I/O operation on closed file.")

        if self._handle:
            # Finalize the current state with flush
            self._obj_writer.append_content(
                content=b"", handle=self._handle, flush=True
            )
            # Reset the handle to prepare for further appends
            self._handle = ""

    @override
    def close(self) -> None:
        """
        Close the writer and finalize the object.
        """
        if not self._closed:
            # Flush the data before closing
            self.flush()
            self._closed = True
