"""
AIS Plugin for PyTorch

PyTorch Dataset and DataLoader for AIS.

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union
from torch.utils.data import Dataset

from aistore import Client
from aistore.pytorch.utils import list_objects_info

# pylint: disable=too-few-public-methods


class AISBaseClass():
    """
    A base class for creating AIS Datasets for PyTorch

    Args:
        client_url(str): AIS endpoint URL
        urls_list(str or List[str]): single or list of url prefixes to load data
    """
    def __init__(self, client_url: str, urls_list: Union[str, List[str]]) -> None:
        self.client = Client(client_url)
        if isinstance(urls_list, str):
            urls_list = [urls_list]
        self._object_info = {}
        self._object_info = list_objects_info(self.client, urls_list)


# pylint: disable=unused-variable


class AISDataset(AISBaseClass, Dataset):
    """
    A map-style dataset for objects in AIS

    Args:
        client_url(str): AIS endpoint URL
        urls_list(str or List[str]): single or list of url prefixes to load data
    """
    def __init__(self, client_url: str, urls_list: Union[str, List[str]]):
        AISBaseClass.__init__(self, client_url, urls_list)

    def __len__(self):
        return len(self._object_info)

    def __getitem__(self, index: int):
        object_name = self._object_info[index]["object"]
        obj = self.client.bucket(bck_name=self._object_info[index]["bck_name"],
                                 provider=self._object_info[index]["provider"]).object(obj_name=object_name).get().read_all()
        return object_name, obj
