"""
ETL to calculate md5 of an object with streaming.
Communication Type: hpush://

Copyright (c) 2022, NVIDIA CORPORATION. All rights reserved.
"""
import hashlib
from aistore import Client

client = Client("http://192.168.49.2:8080")


def before(context):
    context["before"] = hashlib.md5()
    return context


def transform(input_bytes, context):
    context["before"].update(input_bytes)


def after(context):
    return context["before"].hexdigest().encode()


client.etl().init_code(
    transform=transform,
    before=before,
    after=after,
    etl_id="etl-stream3",
    chunk_size=32768,
)


xaction_id = client.bucket("caltech256").transform(
    etl_id="etl-stream3", to_bck="etl-stream1", ext={"jpg": "txt"}
)
client.xaction().wait_for_xaction_finished(xaction_id)
