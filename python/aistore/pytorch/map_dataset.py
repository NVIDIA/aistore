"""
PyTorch Dataset for AIS.

Copyright (c) 2022-2024, NVIDIA CORPORATION. All rights reserved.
"""

from typing import List, Union, Dict

from aistore.sdk import AISSource
from aistore.pytorch.base_map_dataset import AISBaseMapDataset


class AISMapDataset(AISBaseMapDataset):
    """
    A map-style dataset for objects in AIS.
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

    Args:
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        prefix_map (Dict(AISSource, List[str]), optional): Map of AISSource objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object

    NOTE:
        Each object is represented as a tuple of object_name (str) and object_content (bytes)
    """

    def __init__(
        self,
        ais_source_list: Union[AISSource, List[AISSource]] = [],
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
        etl_name: str = None,
    ):
        super().__init__(ais_source_list, prefix_map)
        self._etl_name = etl_name

    def __len__(self):
        return len(self._obj_list)

    def __getitem__(self, index: int):
        try:
            obj = self._obj_list[index]
            content = obj.get(etl_name=self._etl_name).read_all()
            return obj.name, content
        except IndexError:
            raise IndexError(
                f"<{self.__class__.__name__}> index must be in bounds of dataset"
            )
