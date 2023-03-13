#
# Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
#

from abc import abstractmethod, ABC
from typing import Iterator, Dict


class ObjectCollection(ABC):
    """
    Abstract class for collections of object names
    """

    @abstractmethod
    def get_value(self) -> Dict[str, any]:
        """
        Get the json representation of the names to send to the API

        Returns:
            Dictionary of request entry to name representation

        """

    @abstractmethod
    def __iter__(self) -> Iterator[str]:
        pass
