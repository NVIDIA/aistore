"""
AIS IO Datapipe
Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

from io import BytesIO
from typing import Iterator, Tuple

from torchdata.datapipes.iter import IterDataPipe
from torchdata.datapipes import functional_datapipe
from torchdata.datapipes.utils import StreamWrapper

try:
    import aistore
    from aistore.pytorch.utils import parse_url, unparse_url
    from aistore.client import Client
except ImportError:
    aistore = None

# pylint: disable=unused-variable


def _assert_aistore() -> None:
    if aistore is None:
        raise ModuleNotFoundError(
            "Package 'aistore' is required to be installed to use this datapipe."
            "Please run 'pip install aistore' or 'conda install aistore' to install the package"
            "For more info visit: https://github.com/NVIDIA/aistore/blob/master/sdk/python/"
        )


@functional_datapipe("list_files_by_ais")
class AISFileListerIterDataPipe(IterDataPipe[str]):
    """
    Iterable Datapipe that lists files from the AIS backends with the given URL prefixes.
    Acceptable prefixes include but not limited to - ais://bucket-name, ais://bucket-name/,
    ais://bucket-name/folder, ais://bucket-name/folder/, ais://bucket-name/prefix.
    Note:
    -   This function also supports files from multiple backends (aws://.., gcp://.., etc)
    -   Input must be a list and direct URLs are not supported.
    -   length is -1 by default, all calls to len() are invalid as
        not all items are iterated at the start.
    -   This internally uses AIStore Python SDK.

    Args:
        source_datapipe(IterDataPipe[str]): a DataPipe that contains URLs/URL
                                            prefixes to objects on AIS
        length(int): length of the datapipe
        url(str): AIStore endpoint
    """
    def __init__(self, source_datapipe: IterDataPipe[str], url: str, length: int = -1) -> None:
        _assert_aistore()
        self.source_datapipe: IterDataPipe[str] = source_datapipe
        self.length: int = length
        self.client = Client(url)

    def __iter__(self) -> Iterator[str]:
        for prefix in self.source_datapipe:
            provider, bck_name, prefix = parse_url(prefix)
            obj_iter = self.client.list_objects_iter(bck_name=bck_name, provider=provider, prefix=prefix)
            for entry in obj_iter:
                yield unparse_url(provider=provider, bck_name=bck_name, obj_name=entry.name)

    def __len__(self) -> int:
        if self.length == -1:
            raise TypeError(f"{type(self).__name__} instance doesn't have valid length")
        return self.length


# pylint: disable=unused-variable
@functional_datapipe("load_files_by_ais")
class AISFileLoaderIterDataPipe(IterDataPipe[Tuple[str, StreamWrapper]]):
    """
    Iterable Datapipe that loads files from the AIS backends with the given
    list of URLs (no prefixes allowed). Iterates all files in BytesIO format
    and returns a tuple (url, BytesIO).

    Note:
    -   This function also supports files from multiple backends (aws://.., gcp://.., etc)
    -   Input must be a list and direct URLs are not supported.
    -   This internally uses AIStore Python SDK.

    Args:
        source_datapipe(IterDataPipe[str]): a DataPipe that contains URLs/URL prefixes to objects
        length(int): length of the datapipe
        url(str): AIStore endpoint
    """
    def __init__(self, source_datapipe: IterDataPipe[str], url: str, length: int = -1) -> None:
        _assert_aistore()
        self.source_datapipe: IterDataPipe[str] = source_datapipe
        self.length = length
        self.client = Client(url)

    def __iter__(self) -> Iterator[Tuple[str, StreamWrapper]]:
        for url in self.source_datapipe:
            provider, bck_name, obj_name = parse_url(url)
            yield url, StreamWrapper(BytesIO(self.client.get_object(bck_name=bck_name, provider=provider, obj_name=obj_name).read_all()))

    def __len__(self) -> int:
        return len(self.source_datapipe)
