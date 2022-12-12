"""
ETL to calculate md5 of an object.
Communication Type: hpush://

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""
import hashlib
from aistore import Client

client = Client("http://192.168.49.2:8080")


def transform(input_bytes):
    md5 = hashlib.md5()
    md5.update(input_bytes)
    return md5.hexdigest().encode()


client.etl().init_code(transform=transform, etl_id="etl-md5")

xaction_id = client.bucket("from-bck").transform(
    etl_id="etl-md5", to_bck="to-bck", ext={"jpg": "txt"}
)
client.xaction().wait_for_xaction_finished(xaction_id)
