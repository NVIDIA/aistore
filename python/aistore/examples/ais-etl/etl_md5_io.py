"""
ETL to calculate md5 of an object.
Communication Type: io://

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""
from aistore import Client
import hashlib
import sys

client = Client("http://192.168.49.2:8080")


def etl():
    md5 = hashlib.md5()
    chunk = sys.stdin.buffer.read()
    md5.update(chunk)
    sys.stdout.buffer.write(md5.hexdigest().encode())


client.etl().init_code(transform=etl, etl_id="etl-md5-io-code", communication_type="io")

job_id = client.bucket("from-bck").transform(
    etl_id="etl-md5-io-code", to_bck="to-bck", ext={"jpg": "txt"}
)
client.job().wait_for_job(job_id)
