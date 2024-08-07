from typing import Iterator
import requests
from requests.structures import CaseInsensitiveDict
from aistore.sdk.const import DEFAULT_CHUNK_SIZE
from aistore.sdk.object_attributes import ObjectAttributes


class ObjectReader:
    """
    Represents the data returned by the API when getting an object, including access to the content stream and object
    attributes.
    """

    def __init__(
        self,
        response_headers: CaseInsensitiveDict,
        stream: requests.Response,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self._chunk_size = chunk_size
        self._stream = stream
        self._attributes = ObjectAttributes(response_headers)

    @property
    def attributes(self) -> ObjectAttributes:
        """
        Object metadata attributes.

        Returns:
            ObjectAttributes: Parsed object attributes from the headers returned by AIS
        """
        return self._attributes

    def read_all(self) -> bytes:
        """
        Read all byte data from the object content stream.

        This uses a bytes cast which makes it slightly slower and requires all object content to fit in memory at once.

        Returns:
            bytes: Object content as bytes.
        """
        obj_arr = bytearray()
        for chunk in self:
            obj_arr.extend(chunk)
        return bytes(obj_arr)

    def raw(self) -> requests.Response:
        """
        Returns the raw byte stream of object content.

        Returns:
            requests.Response: Raw byte stream of the object content
        """
        return self._stream.raw

    def __iter__(self) -> Iterator[bytes]:
        """
        Creates a generator to read the stream content in chunks.

        Returns:
            Iterator[bytes]: An iterator to access the next chunk of bytes
        """
        try:
            yield from self._stream.iter_content(chunk_size=self._chunk_size)
        finally:
            self._stream.close()
