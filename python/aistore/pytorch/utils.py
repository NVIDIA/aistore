"""
Utils for AIS PyTorch Plugin

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Tuple, Iterable
from urllib.parse import urlparse, urlunparse, parse_qs
from aistore.sdk import Client
from aistore.sdk.ais_source import AISSource
from aistore.sdk.object import Object


def parse_url(url: str) -> Tuple[str, str, str]:
    """
    Parse AIS urls for bucket and object names
    Args:
        url (str): Complete URL of the object (eg. "ais://bucket1/file.txt")
    Returns:
        provider (str): AIS Backend
        bck_name (str): Bucket name identifier
        obj_name (str):  Object name with extension
    """
    parsed_url = urlparse(url)
    path = parsed_url.path
    if len(path) > 0 and path.startswith("/"):
        path = path[1:]

    # returns provider, bck_name, path
    return parsed_url.scheme, parsed_url.netloc, path


# pylint: disable=unused-variable
def list_objects(
    client: Client, urls_list: List[str], ais_source_list: List[AISSource]
) -> List[Object]:
    """
    Create list of all the objects in the given urls and AISSources

    Args:
        client (Client): AIStore client object of the calling method
        urls_list (List[str]): list of urls
        ais_source_list (AISSource, List[AISSource]): list of AISSource objects to load data

    Returns:
        List[Object]: list of all the objects in the given urls and AISSources
    """
    samples = []
    for item in urls_list:
        provider, bck_name, path = parse_url(item)
        objects_iter = client.bucket(
            bck_name=bck_name, provider=provider
        ).list_all_objects_iter(prefix=path)
        for obj in objects_iter:
            samples.append(obj)

    for item in ais_source_list:
        for obj in item.list_all_objects_iter():
            samples.append(obj)

    return samples


def unparse_url(provider: str, bck_name: str, obj_name: str) -> str:
    """
    To generate URL based on provider, bck_name and object name
    Args:
        provider(str): Provider name ('ais', 'gcp', etc)
        bck_name(str): Bucket name
        obj_name(str): Object name with extension.
    Returns:
        unparsed_url(str): Unparsed url (complete url)
    """
    return urlunparse([provider, bck_name, obj_name, "", "", ""])


def list_objects_iterator(
    client: Client,
    urls_list: List[str],
    ais_source_list: List[AISSource],
) -> Iterable[Object]:
    """
    Create an iterable over all the objects in the given urls and AISSources

    Args:
        client (Client): AIStore client object of the calling method
        urls_list (List[str]): list of urls
        ais_source_list (AISSource, List[AISSource]): list of AISSource objects to load data

    Returns:
        Iterable[Object]: iterable over all the objects in the given urls and AISSources
    """
    for item in urls_list:
        provider, bck_name, path = parse_url(item)
        objects_iter = client.bucket(
            bck_name=bck_name, provider=provider
        ).list_all_objects_iter(prefix=path)
        for obj in objects_iter:
            yield obj

    for item in ais_source_list:
        for obj in item.list_all_objects_iter():
            yield obj
