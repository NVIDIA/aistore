"""
ETL to calculate md5 of an object.
Communication Type: hpush://

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""

import hashlib
from aistore import Client
from aistore.sdk import Bucket

client = Client("http://192.168.49.2:8080")


def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()


client.etl("etl-md5").init_code(transform=transform)

job_id = client.bucket("from-bck").transform(
    etl_name="etl-md5", to_bck=Bucket("to-bck"), ext={"jpg": "txt"}
)
client.job(job_id).wait()
