from typing import Literal, Union, Optional
from pathlib import Path

import json
import yaml

from pydantic import BaseModel, root_validator

from aistore.sdk.types import BucketModel
from aistore.sdk.multiobj import ObjectNames, ObjectRange
from aistore.sdk.dsort.ekm import ExternalKeyMap


# pylint: disable=redefined-builtin,too-few-public-methods
class DsortShardsGroup:
    """
    Represents the configuration for the input or output of a shard group in a dSort job
    """

    def __init__(
        self,
        bck: BucketModel,
        role: Literal["input", "output"],
        format: Union[ObjectNames, ObjectRange, ExternalKeyMap],
        extension: str,
    ):
        if role not in ["input", "output"]:
            raise ValueError('role must be either "input" or "output"')
        if role == "input" and not isinstance(format, (ObjectNames, ObjectRange)):
            raise ValueError(
                'format of input role must be an instance of "ObjectNames" or "ObjectRange"'
            )
        if role == "output" and not isinstance(format, (ObjectRange, ExternalKeyMap)):
            raise ValueError(
                'format of output role must be an instance of "ObjectRange" or "ExternalKeyMap"'
            )

        self.bck = bck
        self.role = role
        self.extension = extension
        self.format = format

    def as_dict(self):
        """
        Converts the DsortShardsGroup instance to a dictionary representation
        """

        return {
            f"{self.role}_bck": self.bck.as_dict(),
            f"{self.role}_format": (
                self.format.get_value() if self.role == "input" else str(self.format)
            ),
            f"{self.role}_extension": self.extension,
        }


class DsortAlgorithm(BaseModel):
    """
    Represents the algorithm used in a Dsort job
    """

    kind: Literal["alphanumeric", "shuffle", "content"] = "alphanumeric"
    decreasing: bool = False
    seed: Optional[str] = ""
    extension: Optional[str] = None
    content_key_type: Optional[Literal["int", "float", "string"]] = None

    # pylint: disable=no-self-argument
    @root_validator
    def validate_content_fields(cls, values):
        """
        Validates required key fields
        """
        kind = values.get("kind")
        extension = values.get("extension")
        content_key_type = values.get("content_key_type")

        if kind == "content":
            if not extension:
                raise ValueError(
                    'For kind="content", the "extension" field is required.'
                )
            if not content_key_type:
                raise ValueError(
                    'For kind="content", the "content_key_type" field is required.'
                )
        else:
            if extension or content_key_type:
                raise ValueError(
                    'The "extension" and "content_key_type" fields are only allowed for kind="content".'
                )

        return values

    def as_dict(self):
        """
        Converts the DsortAlgorithm instance to a dictionary representation
        """
        dict_rep = {"kind": self.kind, "decreasing": self.decreasing, "seed": self.seed}
        if self.kind == "content":
            dict_rep["extension"] = self.extension
            dict_rep["content_key_type"] = self.content_key_type
        return dict_rep


class DsortFramework:
    """
    Represents the framework for a dSort job, including input and output shard configurations.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        input_shards: DsortShardsGroup,
        output_shards: DsortShardsGroup,
        algorithm: DsortAlgorithm = None,
        description=None,
        output_shard_size=None,
    ) -> None:
        self.input_shards = input_shards
        self.output_shards = output_shards
        self.output_shard_size = output_shard_size
        if algorithm is None:
            algorithm = DsortAlgorithm()
        self.algorithm = algorithm
        self.description = description

        self.ekm_file = None
        self.ekm_sep = None

    @classmethod
    def from_file(cls, spec):
        """
        Class method to create a DsortFramework instance from a JSON or YAML file

        Args:
            spec (str or Path): The path to the JSON or YAML file containing the specification

        Returns:
            DsortFramework: An instance of the DsortFramework class

        Raises:
            ValueError: If the file extension is not .json, .yml, or .yaml
        """
        spec_path = Path(spec)
        ext = spec_path.suffix.lower()
        if ext not in [".json", ".yaml", ".yml"]:
            raise ValueError(
                "Unsupported file format. Please provide a .json, .yml, or .yaml file."
            )
        load_func = json.load if ext == ".json" else yaml.safe_load
        with open(spec, "r", encoding="utf-8") as file_data:
            spec_data = load_func(file_data)

            # Parse and validate input_format
            input_format = spec_data.get("input_format", None)
            if not isinstance(input_format, dict):
                raise ValueError("input_format must be a dictionary")

            if "template" in input_format:
                input_format = ObjectRange.from_string(input_format["template"])
            elif "objnames" in input_format:
                input_format = ObjectNames(input_format["objnames"])
            else:
                raise ValueError(
                    '"input_format" dictionary must contain either the key "template" or "objnames"'
                )

            # Parse and validate output_format
            output_format = spec_data.get("output_format", None)
            if isinstance(output_format, list):
                output_format = ObjectNames(output_format)
            elif isinstance(output_format, str):
                output_format = ObjectRange.from_string(output_format)
            else:
                raise ValueError(
                    '"input_format" string must be either a list or a range string'
                )

            framework = cls(
                input_shards=DsortShardsGroup(
                    bck=BucketModel(**spec_data.get("input_bck")),
                    role="input",
                    format=input_format,
                    extension=spec_data.get("input_extension", ""),
                ),
                output_shards=DsortShardsGroup(
                    bck=BucketModel(**spec_data.get("output_bck")),
                    role="output",
                    format=output_format,
                    extension=spec_data.get("output_extension", ""),
                ),
                algorithm=DsortAlgorithm(**spec_data.get("algorithm", {})),
                output_shard_size=spec_data.get("output_shard_size", ""),
            )

            framework.ekm_file = spec_data.get("ekm_file", "")
            framework.ekm_sep = spec_data.get("ekm_sep", "")

            return framework

    def to_spec(self):
        """
        Converts the DsortFramework instance to a dictionary representation for use in dSort job specification

        Returns:
            Dictionary representation of dSort specification
        """
        spec = {**self.input_shards.as_dict(), **self.output_shards.as_dict()}
        spec["algorithm"] = self.algorithm.as_dict()
        spec["description"] = self.description if self.description else ""
        if self.output_shard_size:
            spec["output_shard_size"] = self.output_shard_size

        if self.ekm_file:
            spec["ekm_file"] = self.ekm_file
            spec["ekm_sep"] = self.ekm_sep

        return spec
