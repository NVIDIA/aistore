"""
ETL to calculate md5 of an object.
Communication Type: io://

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""
from aistore import Client
import hashlib
import sys

from aistore.sdk import Bucket
from aistore.sdk.etl_const import ETL_COMM_IO

client = Client("http://192.168.49.2:8080")


def etl():
    md5 = hashlib.md5()
    chunk = sys.stdin.buffer.read()
    md5.update(chunk)
    sys.stdout.buffer.write(md5.hexdigest().encode())


client.etl().init_code(
    transform=etl, etl_name="etl-md5-io-code", communication_type=ETL_COMM_IO
)

job_id = client.bucket("from-bck").transform(
    etl_name="etl-md5-io-code", to_bck=Bucket("to-bck"), ext={"jpg": "txt"}
)
client.job(job_id).wait()
