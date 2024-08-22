from typing import Dict, List

from aistore.sdk.multiobj import ObjectNames

EKM_FILE_NAME = "ekm-file.json"


class ExternalKeyMap(Dict[str, ObjectNames]):
    """
    A dictionary-like class for managing external key mappings in dSort operations
    """

    def __setitem__(self, key: str, value: ObjectNames) -> None:
        if not isinstance(key, str):
            raise TypeError(f"Key must be a string, got {type(key).__name__}")
        if not isinstance(value, ObjectNames):
            raise TypeError(
                f"Value must be an instance of ObjectNames, got {type(value).__name__}"
            )
        super().__setitem__(key, value)

    def as_dict(self) -> Dict[str, List[str]]:
        """
        Converts the ExternalKeyMap to a dictionary representation
        """
        return {
            shard_format: objs.get_value()["objnames"]
            for shard_format, objs in self.items()
        }

    def __str__(self):
        # If the format is ExternalKeyMap, `output_format` is omitted and here used an empty string as placeholder
        return ""
