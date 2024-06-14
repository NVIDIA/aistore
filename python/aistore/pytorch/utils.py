"""
Utils for AIS PyTorch Plugin

Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Iterable, Tuple
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
) -> Iterable[bytes]:
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
            if obj.name != path:
                obj_name = obj.name.replace(f"{path}/", "", 1)
                yield bucket.object(path).get(
                    etl_name=etl_name,
                    archive_settings=ArchiveSettings(archpath=obj_name),
                ).read_all()


def get_basename(name: str) -> str:
    """
    Get the basename of the object name by stripping any directory information and suffix.

    Args:
        name (str): Complete object name

    Returns:
        str: Basename of the object
    """

    return name.split("/")[-1].split(".")[0]


def __samples_from_bck_iter(shard_name: str, bucket: Bucket, etl_name: str):
    """
    Helper function to create an iterator for all samples and contents over the given shard name.

    Args:
        name (str): Name of shard object
        bucket (Bucket): Bucket where the shard object is stored
        etl_name (str): Name of ETL (Extract, Transform, Load) to apply to each object

    Returns:
        Iterable[Tuple[str, dict(str, bytes)]]: Iterator over all the WDS basenames and content (file extension, data)
        in shards from the given shard
    """
    # get iterator of all objects in the shard
    objects_iter = bucket.list_objects_iter(
        prefix=shard_name, props="name", flags=[ListObjectFlag.ARCH_DIR]
    )

    # pool all files with the same basename into dictionary (basename, [file names])
    samples_dict = {}
    for obj in objects_iter:
        basename = get_basename(obj.name)

        # Original tar is included in basenames so only yield actual files
        if basename != shard_name.split(".")[0]:
            if basename not in samples_dict:
                samples_dict[basename] = []
            samples_dict[basename].append(obj.name)

    # for each basename, get the byte data for each file and yield in dictionary
    shard = bucket.object(shard_name)
    for basename, files in samples_dict.items():
        content_dict = {}
        for file_name in files:
            file_prefix = file_name.split(".")[-1]
            content_dict[file_prefix] = shard.get(
                etl_name=etl_name, archive_settings=ArchiveSettings(archpath=file_name)
            ).read_all()
        yield basename, content_dict


def list_wds_samples_iter(
    client: Client,
    urls_list: List[str],
    bucket_list: List[Bucket],
    etl_name: str,
) -> Iterable[Tuple[str, bytes]]:
    """
    Create an iterator over all of the shard sample basenames and sample contents.

    Args:
        client (Client): AIStore Client for accessing buckets and objects
        urls_list (List[str]): List of URLs, can be URLS for buckets and/or objects
        bucket_list (List[Bucket]): List of Bucket objects containing the shards to load data
        etl_name (str): Name of ETL (Extract, Transform, Load) to apply to each object

    Returns:
        Iterable[Tuple[str, dict(str, bytes)]]: Iterator over all the WDS basenames and content (file extension, data)
        in shards from the given URLs and buckets
    """

    for item in urls_list:
        provider, bck_name, path = parse_url(item)
        bucket = client.bucket(bck_name=bck_name, provider=provider)
        if path == None or path == "":
            for shard in bucket.list_objects_iter():
                yield from __samples_from_bck_iter(shard.name, bucket, etl_name)
        else:
            yield from __samples_from_bck_iter(path, bucket, etl_name)

    for bucket in bucket_list:
        for shard in bucket.list_objects_iter():
            yield from __samples_from_bck_iter(shard.name, bucket, etl_name)
