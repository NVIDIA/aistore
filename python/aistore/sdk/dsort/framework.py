from typing import Literal, Dict, Union, Optional
from pathlib import Path

import json
import yaml

from pydantic import BaseModel, root_validator

from aistore.sdk.types import BucketModel


class DsortShardsGroup(BaseModel):
    """
    Represents the configuration for the input or output of a shard group in a dSort job
    """

    bck: BucketModel
    role: Literal["input", "output"]
    format: Union[Dict[str, str], str]
    extension: str

    # pylint: disable=no-self-argument
    @root_validator(allow_reuse=True)
    def check_key(cls, values):
        """
        Validates that if the role contains required key fields
        """

        role = values.get("role")
        format_value = values.get("format")

        if role == "input":
            if not isinstance(format_value, dict) or "template" not in format_value:
                raise ValueError(
                    'For input shards, format must be a dictionary containing the key "template".'
                )
        elif role == "output":
            if not isinstance(format_value, str):
                raise ValueError("For output shards, format must be a string.")

        return values

    def as_dict(self):
        """
        Converts the DsortShardsGroup instance to a dictionary representation
        """

        return {
            f"{self.role}_bck": self.bck.as_dict(),
            f"{self.role}_format": self.format,
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
        algorithm: DsortAlgorithm,
        description=None,
        output_shard_size=None,
    ) -> None:
        self.input_shards = input_shards
        self.output_shards = output_shards
        self.output_shard_size = output_shard_size
        self.algorithm = algorithm
        self.description = description

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
            return cls(
                input_shards=DsortShardsGroup(
                    bck=BucketModel(**spec_data.get("input_bck")),
                    role="input",
                    format=spec_data.get("input_format", {}),
                    extension=spec_data.get("input_extension", ""),
                ),
                output_shards=DsortShardsGroup(
                    bck=BucketModel(**spec_data.get("output_bck")),
                    role="output",
                    format=spec_data.get("output_format", ""),
                    extension=spec_data.get("output_extension", ""),
                ),
                algorithm=DsortAlgorithm(**spec_data.get("algorithm", {})),
                output_shard_size=spec_data.get("output_shard_size", ""),
            )

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
        return spec
