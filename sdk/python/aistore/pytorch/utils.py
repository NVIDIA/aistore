"""
Utils for AIS PyTorch Plugin

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Mapping
from urllib.parse import urlparse, urlunparse
from aistore.client import Client


def parse_url(url: str) -> Mapping[str, str]:
    """
    Parse AIS urls for bucket and object names
    Args:
        url (str): Complete URL of the object (eg. "ais://bucket1/file.txt")
    Returns:
        Mapping[str, str]: Map containing information related to provider, bucket name and object path
    """
    parsed_url = urlparse(url)
    path = parsed_url.path
    if len(path) > 0 and path.startswith("/"):
        path = path[1:]

    url_info = {"provider": parsed_url.scheme, "bck_name": parsed_url.netloc, "path": path}
    return url_info


# pylint: disable=unused-variable
def list_objects_info(client: Client, urls_list: List[str]) -> List[Mapping[str, str]]:
    """
    Create list of list of [bucket_name, object_name] from all the object urls
    Args:
        client (Client): AIStore client object of the calling method
        urls_list (List[str]): list of urls
    Returns:
        List[samples](List[Mapping[str, str]]): list of {provider, bucket, path to the object}
    """
    samples = []
    for url in urls_list:
        url_info = parse_url(url)
        provider, bck_name = url_info["provider"], url_info["bck_name"]
        objects = client.list_objects(bck_name=bck_name, prefix=url_info["path"], provider=provider)
        for obj_info in objects.entries:
            samples.append({"provider": provider, "bck_name": bck_name, "object": obj_info.name})
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
    return urlunparse([provider, bck_name, obj_name, '', '', ''])
