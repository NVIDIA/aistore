"""
Iterable Dataset for AIS

Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
"""

from aistore.pytorch.base_iter_dataset import AISBaseIterDataset
from typing import List, Union, Dict
from aistore.sdk import AISSource
from alive_progress import alive_it


class AISIterDataset(AISBaseIterDataset):
    """
    An iterable-style dataset that iterates over objects in AIS and yields
    samples represented as a tuple of object_name (str) and object_content (bytes).
    If `etl_name` is provided, that ETL must already exist on the AIStore cluster.

    Args:
        ais_source_list (Union[AISSource, List[AISSource]]): Single or list of AISSource objects to load data
        prefix_map (Dict(AISSource, Union[str, List[str]]), optional): Map of AISSource objects to list of prefixes that only allows
        objects with the specified prefixes to be used from each source
        etl_name (str, optional): Optional ETL on the AIS cluster to apply to each object
        show_progress (bool, optional): Enables console dataset reading progress indicator

    Yields:
        Tuple[str, bytes]: Each item is a tuple where the first element is the name of the object and the
        second element is the byte representation of the object data.
    """

    def __init__(
        self,
        ais_source_list: Union[AISSource, List[AISSource]],
        prefix_map: Dict[AISSource, Union[str, List[str]]] = {},
        etl_name: str = None,
        show_progress: bool = False,
    ):
        super().__init__(ais_source_list, prefix_map)
        self._etl_name = etl_name
        self._show_progress = show_progress

    def __iter__(self):
        self._reset_iterator()
        # Get iterator for current worker and name (if no workers, just entire iter)
        worker_iter, worker_name = self._get_worker_iter_info()

        # For object, yield name and content
        for obj in alive_it(
            worker_iter,
            title="AISIterDataset" + worker_name,
            disable=not self._show_progress,
            force_tty=worker_name == "",
        ):
            yield obj.name, obj.get(etl_name=self._etl_name).read_all()
