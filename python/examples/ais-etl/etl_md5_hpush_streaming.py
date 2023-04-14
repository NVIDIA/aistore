"""
ETL to calculate md5 of an object with streaming.
Communication Type: hpush://

Copyright (c) 2022-2023, NVIDIA CORPORATION. All rights reserved.
"""
import hashlib
from aistore import Client
from aistore.sdk import Bucket

client = Client("http://192.168.49.2:8080")


def transform(reader, writer):
    checksum = hashlib.md5()
    for b in reader:
        checksum.update(b)
    writer.write(checksum.hexdigest().encode())


client.etl("etl-stream").init_code(
    transform=transform,
    chunk_size=32768,
)


job_id = client.bucket("from-bck").transform(
    etl_name="etl-stream", to_bck=Bucket("to-bck"), ext={"jpg": "txt"}
)
client.job(job_id).wait()
