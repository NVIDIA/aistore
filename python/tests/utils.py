import os
import random
import shutil
import string
import tarfile
import io
from pathlib import Path

from typing import Dict, List

from aistore.sdk import Client
from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.errors import ErrBckNotFound


# pylint: disable=unused-variable
def random_string(length: int = 10):
    return "".join(random.choices(string.ascii_lowercase, k=length))


def string_to_dict(input_string: str) -> Dict:
    pairs = input_string.split(", ")
    result_dict = {
        key_value.split("=")[0]: key_value.split("=")[1] for key_value in pairs
    }
    return result_dict


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
    client.bucket(bck_name, provider=provider).object(obj_name).put_content(content)
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


def create_random_tarballs(
    num_files: int, num_extensions: int, min_shard_size: int, dest_dir: str
):
    def generate_random_string(length: int) -> str:
        return "".join(random.choices(string.ascii_letters + string.digits, k=length))

    def generate_random_content(min_size: int = 1024, max_size: int = 10240) -> bytes:
        size = random.randint(min_size, max_size)
        return os.urandom(size)

    def generate_files(num_files: int, num_extensions: int, dest_dir: str) -> List:
        files_list = []
        filenames_list = []

        dest_dir_path = Path(dest_dir)
        dest_dir_path.mkdir(parents=True, exist_ok=True)

        extension_list = [generate_random_string(3) for _ in range(num_extensions)]
        for _ in range(num_files):
            filename = generate_random_string(10)
            filenames_list.append(filename)

            for ext in extension_list:
                file_path = dest_dir_path / f"{filename}.{ext}"
                with open(file_path, "wb") as file:
                    file.write(generate_random_content())
                files_list.append(file_path)

        return files_list, extension_list

    def create_tarballs(min_shard_size: int, dest_dir: str, files_list: List) -> None:
        num_input_shards = 0
        current_size = 0
        dest_dir_path = Path(dest_dir).resolve()
        current_tarball = dest_dir_path / f"input-shard-{num_input_shards}.tar"
        total_size = 0
        file_count = 0
        tarball_info = []

        random.shuffle(files_list)

        for file in files_list:
            file_size = file.stat().st_size
            if current_size > min_shard_size:
                tarball_info.append(
                    f"{current_tarball.name}\t{current_size}\t{file_count}"
                )
                num_input_shards += 1
                current_tarball = dest_dir_path / f"input-shard-{num_input_shards}.tar"
                current_size = 0
                file_count = 0

            with tarfile.open(current_tarball, "a") as tar:
                tar.add(file, arcname=file.name)

            current_size += file_size
            file_count += 1
            total_size += file_size

            file.unlink()

        tarball_info.append(f"{current_tarball.name}\t{current_size}\t{file_count}")
        return num_input_shards

    filename_list, extension_list = generate_files(num_files, num_extensions, dest_dir)
    num_input_shards = create_tarballs(min_shard_size, dest_dir, filename_list)
    filename_list = list(map(lambda filepath: filepath.stem, filename_list))
    return filename_list, extension_list, num_input_shards
