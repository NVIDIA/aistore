from typing import Iterator, List, Dict, Optional
import requests

from aistore.sdk.request_client import RequestClient
from aistore.sdk.const import DEFAULT_CHUNK_SIZE, HTTP_METHOD_GET, HTTP_METHOD_HEAD
from aistore.sdk.object_attributes import ObjectAttributes


class ObjectReader:
    """
    Represents the data returned by the API when getting an object, including access to the content stream and object
    attributes.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        client: RequestClient,
        path: str,
        params: List[str],
        headers: Optional[Dict[str, str]] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        self._request_client = client
        self._request_path = path
        self._request_params = params
        self._request_headers = headers
        self._chunk_size = chunk_size
        self._attributes = None

    def _head(self) -> ObjectAttributes:
        """
        Make a head request to AIS to update and return only object attributes.

        Returns:
            ObjectAttributes for this object

        """
        resp = self._request_client.request(
            HTTP_METHOD_HEAD, path=self._request_path, params=self._request_params
        )
        return ObjectAttributes(resp.headers)

    def _make_request(self, stream) -> requests.Response:
        """
        Make a request to AIS to get the object content.

        Returns:
            requests.Response from AIS

        """
        resp = self._request_client.request(
            HTTP_METHOD_GET,
            path=self._request_path,
            params=self._request_params,
            stream=stream,
            headers=self._request_headers,
        )
        self._attributes = ObjectAttributes(resp.headers)
        return resp

    @property
    def attributes(self) -> ObjectAttributes:
        """
        Object metadata attributes.

        Returns:
            ObjectAttributes: Parsed object attributes from the headers returned by AIS
        """
        if not self._attributes:
            self._attributes = self._head()
        return self._attributes

    def read_all(self) -> bytes:
        """
        Read all byte data directly from the object response without using a stream.

        This requires all object content to fit in memory at once and downloads all content before returning.

        Returns:
            bytes: Object content as bytes.
        """
        return self._make_request(stream=False).content

    def raw(self) -> requests.Response:
        """
        Returns the raw byte stream of object content.

        Returns:
            requests.Response: Raw byte stream of the object content
        """
        return self._make_request(stream=True).raw

    def __iter__(self) -> Iterator[bytes]:
        """
        Make a request to get a stream from the provided object and yield chunks of the stream content.

        Returns:
            Iterator[bytes]: An iterator over each chunk of bytes in the object
        """
        stream = self._make_request(stream=True)
        try:
            yield from stream.iter_content(chunk_size=self._chunk_size)
        finally:
            stream.close()
