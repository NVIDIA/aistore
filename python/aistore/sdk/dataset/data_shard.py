#
# Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
#

from dataclasses import dataclass
from aistore.sdk.client import Client
from aistore.sdk.provider import Provider


@dataclass
class DataShard:
    """
    A class representing a group of data shards stored in AIStore

    Attributes:
        client_url (str): AIS endpoint URL
        bucket_name (str): The name of the bucket containing the data shards
        provider (str): The provider of the bucket, default is "ais"
        prefix (str): The prefix for the data shards, default is ""
        etl_name (str): The name of the ETL to apply to the data shards, default is None
    """

    client_url: str
    bucket_name: str
    provider: str = Provider.AIS
    prefix: str = ""
    etl_name: str = None

    def __post_init__(self):
        self.client = Client(self.client_url)
        self.bucket = self.client.bucket(
            bck_name=self.bucket_name, provider=self.provider
        )
