#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#
from dataclasses import dataclass
from enum import Enum


class ArchiveMode(Enum):
    """
    Archive mode for getting files/objects from an archive in a bucket
    See `MatchMode` enum in the cmn/archive/read.go
    """

    REGEXP = "regexp"
    PREFIX = "prefix"
    SUFFIX = "suffix"
    SUBSTR = "substr"
    WDSKEY = "wdskey"


@dataclass
class ArchiveConfig:
    """
    Configuration for extracting files from an archive

    Attributes:
    archpath (str, optional): If the object is an archive, use `archpath` to extract a single file
        from the archive
    regex (str, optional): A prefix, suffix, WebDataset key, or general-purpose regular expression
        used to match filenames within the archive and select possibly multiple files
    mode (ArchiveMode, optional): Specifies the mode of archive extraction when using `regex`

    Example:
        # Extract a single file from an archive
        single_file_settings = ArchiveConfig(
            archpath="path/to/your/file.txt"
        )

        # Extract multiple files from an archive
        multi_file_settings = ArchiveConfig(
            regex = "log",  # Retrieve all log files from the archive
            mode=ArchiveMode.SUFFIX,
        )
    """

    archpath: str = ""
    regex: str = ""
    mode: ArchiveMode = None

    def __post_init__(self):
        if self.mode and not self.regex:
            raise ValueError("Archive mode requires archive regex")

        if self.regex and not self.mode:
            raise ValueError("Archive regex requires archive mode")

        if self.regex and self.archpath:
            raise ValueError("Cannot use both Archive regex and Archive path")
