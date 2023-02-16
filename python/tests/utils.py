import os
import random
import shutil
import string
import tempfile
from pathlib import Path

from aistore.sdk import Client
from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.errors import ErrBckNotFound


# pylint: disable=unused-variable
def random_string(length: int = 10):
    return "".join(random.choices(string.ascii_lowercase, k=length))


# pylint: disable=unused-variable
def create_and_put_object(
    client: Client,
    bck_name: str,
    obj_name: str,
    provider: str = "ais",
    obj_size: int = 0,
):
    obj_size = obj_size if obj_size else random.randrange(10, 20)
    obj_body = "".join(random.choices(string.ascii_letters, k=obj_size))
    content = obj_body.encode(UTF_ENCODING)
    temp_file = Path(tempfile.gettempdir()).joinpath(os.urandom(24).hex())
    with open(temp_file, "wb") as file:
        file.write(content)
        file.flush()
        client.bucket(bck_name, provider=provider).object(obj_name).put_file(file.name)
    return content


def destroy_bucket(client: Client, bck_name: str):
    try:
        client.bucket(bck_name).delete()
    except ErrBckNotFound:
        pass


def cleanup_local(path: str):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass
