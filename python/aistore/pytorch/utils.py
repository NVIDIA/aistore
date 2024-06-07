"""
Utils for AIS PyTorch Plugin

Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Iterable
from urllib.parse import urlunparse
from aistore.sdk import Client
from aistore.sdk.ais_source import AISSource
from aistore.sdk.list_object_flag import ListObjectFlag
from aistore.sdk.object import Object
from aistore.sdk.bucket import Bucket
from aistore.sdk.types import ArchiveSettings
from aistore.sdk.utils import parse_url


def list_objects(
    client: Client, urls_list: List[str], ais_source_list: List[AISSource]
) -> List[Object]:
    """
    Create a list of all the objects in the given URLs and AIS sources.

    Args:
        client (Client): AIStore client object
        urls_list (List[str]): List of URLs
        ais_source_list (List[AISSource]): List of AISSource objects to load data

    Returns:
        List[Object]: List of all the objects in the given URLs and AIS sources
    """
    samples = []

    for url in urls_list:
        provider, bck_name, path = parse_url(url)
        bucket = client.bucket(bck_name=bck_name, provider=provider)
        samples.extend([obj for obj in bucket.list_all_objects_iter(prefix=path)])

    for source in ais_source_list:
        samples.extend([obj.name for obj in source.list_all_objects_iter()])

    return samples


def unparse_url(provider: str, bck_name: str, obj_name: str) -> str:
    """
    Generate URL based on provider, bucket name, and object name.

    Args:
        provider (str): Provider name ('ais', 'gcp', etc.)
        bck_name (str): Bucket name
        obj_name (str): Object name with extension

    Returns:
        str: Complete URL
    """
    return urlunparse([provider, bck_name, obj_name, "", "", ""])


def list_objects_iterator(
    client: Client, urls_list: List[str] = [], ais_source_list: List[AISSource] = []
) -> Iterable[Object]:
    """
    Create an iterable over all the objects in the given URLs and AIS sources.

    Args:
        client (Client): AIStore client object
        urls_list (List[str]): List of URLs
        ais_source_list (List[AISSource]): List of AISSource objects to load data

    Returns:
        Iterable[Object]: Iterable over all the objects in the given URLs and AIS sources
    """
    for url in urls_list:
        provider, bck_name, path = parse_url(url)
        bucket = client.bucket(bck_name=bck_name, provider=provider)
        yield from bucket.list_all_objects_iter(prefix=path)

    for source in ais_source_list:
        yield from source.list_all_objects_iter()


def list_shard_objects_iterator(
    bucket: Bucket, prefix: str = "", etl_name: str = ""
) -> Iterable[Object]:
    """
    Create an iterable over all the objects in the given shards.

    Args:
        bucket (Bucket): Bucket containing the shards
        prefix (str): Prefix of the object names
        etl_name (str): ETL name to apply on each object

    Returns:
        Iterable[Object]: Iterable over all the objects in the given shards,
                          with each iteration returning a combined sample
    """
    shards_iter = bucket.list_objects_iter(prefix=prefix, props="name")

    for shard in shards_iter:
        path = shard.name
        objects_iter = bucket.list_objects_iter(
            prefix=path, props="name", flags=[ListObjectFlag.ARCH_DIR]
        )

        for obj in objects_iter:
            if obj.name == path:
                continue
            obj_name = obj.name.replace(f"{path}/", "", 1)
            yield bucket.object(path).get(
                etl_name=etl_name,
                archive_settings=ArchiveSettings(archpath=obj_name),
            ).read_all()
