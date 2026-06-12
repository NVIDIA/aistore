#
# Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
#

from __future__ import annotations

from typing import Any, Dict, Optional, Tuple

try:
    from fsspec.spec import AbstractFileSystem
except ImportError as err:  # pragma: no cover - exercised only when optional extra is missing
    raise ImportError(
        "AISFileSystem requires the optional fsspec dependency. "
        "Install it with `pip install aistore[fsspec]`."
    ) from err

from aistore.sdk import Client
from aistore.sdk.provider import Provider


class AISFileSystem(AbstractFileSystem):
    """
    Read-only fsspec adapter for AIStore objects.

    Paths are expected in the form ``ais://bucket/object`` or ``bucket/object``.
    """

    protocol = "ais"

    def __init__(
        self,
        endpoint: str,
        *,
        provider: str = Provider.AIS.value,
        client: Optional[Client] = None,
        **client_kwargs: Any,
    ):
        super().__init__()
        self._client = client or Client(endpoint, **client_kwargs)
        self._provider = provider

    @staticmethod
    def _strip_protocol(path: str) -> str:
        if path.startswith("ais://"):
            return path[len("ais://") :]
        return path

    def _split_path(self, path: str) -> Tuple[str, str]:
        clean_path = self._strip_protocol(path).lstrip("/")
        if not clean_path:
            raise ValueError("AIS path must include a bucket name")

        bucket, _, object_name = clean_path.partition("/")
        return bucket, object_name

    def _bucket(self, bucket_name: str):
        return self._client.bucket(bucket_name, provider=self._provider)

    def ls(self, path: str, detail: bool = True, **kwargs: Any):
        bucket_name, prefix = self._split_path(path)
        entries = self._bucket(bucket_name).list_objects_iter(
            prefix=prefix, props="name,size"
        )

        infos = [
            {
                "name": f"{bucket_name}/{entry.name}",
                "size": entry.size,
                "type": "file",
            }
            for entry in entries
        ]
        return infos if detail else [info["name"] for info in infos]

    def info(self, path: str, **kwargs: Any) -> Dict[str, Any]:
        bucket_name, object_name = self._split_path(path)
        if not object_name:
            return {
                "name": bucket_name,
                "size": 0,
                "type": "directory",
            }

        props = self._bucket(bucket_name).object(object_name).props
        return {
            "name": f"{bucket_name}/{object_name}",
            "size": props.size,
            "type": "file",
        }

    def cat_file(
        self,
        path: str,
        start: Optional[int] = None,
        end: Optional[int] = None,
        **kwargs: Any,
    ) -> bytes:
        bucket_name, object_name = self._split_path(path)
        if not object_name:
            raise ValueError("AIS object path must include an object name")

        byte_range = None
        if start is not None or end is not None:
            left = "" if start is None else str(start)
            right = "" if end is None else str(end - 1)
            byte_range = f"bytes={left}-{right}"

        return (
            self._bucket(bucket_name)
            .object(object_name)
            .get_reader(byte_range=byte_range)
            .read_all()
        )

    def _open(
        self,
        path: str,
        mode: str = "rb",
        block_size: Optional[int] = None,
        **kwargs: Any,
    ):
        if mode != "rb":
            raise NotImplementedError(
                "AIS fsspec adapter currently supports read-only binary mode"
            )

        bucket_name, object_name = self._split_path(path)
        if not object_name:
            raise ValueError("AIS object path must include an object name")

        return self._bucket(bucket_name).object(object_name).get_reader().as_file()

    def mkdir(self, path: str, create_parents: bool = True, **kwargs: Any) -> None:
        raise NotImplementedError("AIS fsspec adapter does not create buckets")

    def rm(
        self, path: str, recursive: bool = False, maxdepth: Optional[int] = None
    ) -> None:
        raise NotImplementedError("AIS fsspec adapter is read-only")

    def cp_file(self, path1: str, path2: str, **kwargs: Any) -> None:
        raise NotImplementedError("AIS fsspec adapter is read-only")
