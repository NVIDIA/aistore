import os
import random
import shutil
import string
import tempfile
import tarfile
import io
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


# pylint: disable=too-many-arguments
def create_and_put_objects(
    client, bucket, prefix, suffix, num_obj, obj_names, obj_size=None
):
    if not obj_names:
        obj_names = [prefix + str(i) + suffix for i in range(num_obj)]
    for obj_name in obj_names:
        create_and_put_object(
            client,
            bck_name=bucket.name,
            provider=bucket.provider,
            obj_name=obj_name,
            obj_size=obj_size,
        )
    return obj_names


def test_cases(*args):
    def decorator(func):
        def wrapper(self, *inner_args, **kwargs):
            for arg in args:
                with self.subTest(arg=arg):
                    func(self, arg, *inner_args, **kwargs)

        return wrapper

    return decorator


def create_archive(archive_name, content_dict):
    directory = os.path.dirname(archive_name)
    if not os.path.exists(directory):
        os.makedirs(directory)

    with tarfile.open(archive_name, "w") as tar:
        for file_name, file_content in content_dict.items():
            info = tarfile.TarInfo(name=file_name)
            info.size = len(file_content)
            tar.addfile(tarinfo=info, fileobj=io.BytesIO(file_content))
