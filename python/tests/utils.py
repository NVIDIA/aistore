import os
import random
import shutil
import string
import tarfile
import io
from itertools import product
from pathlib import Path
from unittest.mock import Mock

from typing import Any, Callable, Dict, List, Iterator, Tuple
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
)

import requests
from requests.exceptions import ChunkedEncodingError

from aistore.sdk import Client, Object
from aistore.sdk.const import UTF_ENCODING
from aistore.sdk.obj.content_iterator import ContentIterator
from aistore.sdk.types import BucketModel
from tests.const import KB
from tests.integration.sdk import DEFAULT_TEST_CLIENT


# pylint: disable=too-few-public-methods
class BadContentStream(io.BytesIO):
    """
    Simulates a stream that fails intermittently with a specified error after a set number of reads.

    Args:
        data (bytes): The data to be streamed.
        fail_on_read (int): The number of reads after which the error is raised.
        error (Exception): The error instance to raise after `fail_on_read` reads.
    """

    def __init__(self, data: bytes, fail_on_read: int, error: Exception):
        super().__init__(data)
        self.read_count = 0
        self.fail_on_read = fail_on_read
        self.error = error

    def read(self, size: int = -1) -> bytes:
        """Overrides `BytesIO.read` to raise an error after a specific number of reads."""
        self.read_count += 1
        if self.read_count == self.fail_on_read:
            raise self.error
        return super().read(size)


# pylint: disable=too-few-public-methods
class BadContentIterator(ContentIterator):
    """
    Simulates a ContentIterator that streams data in chunks and intermittently raises errors
    via a `BadContentStream`.

    Args:
        data (bytes): The data to be streamed in chunks.
        fail_on_read (int): The number of reads after which an error will be raised.
        chunk_size (int): The size of each chunk to be read from the data.
        error (Exception): The error instance to raise after `fail_on_read` reads.
    """

    def __init__(
        self,
        data: bytes,
        fail_on_read: int,
        chunk_size: int,
        error: Exception = ChunkedEncodingError("Simulated ChunkedEncodingError"),
    ):
        super().__init__(client=Mock(), chunk_size=chunk_size)
        self.data = data
        self.fail_on_read = fail_on_read
        self.error = error
        self.read_position = 0

    def iter(self, offset: int = 0) -> Iterator[bytes]:
        """Streams data using `BadContentStream`, starting from `offset`."""
        stream = BadContentStream(
            self.data[offset:], fail_on_read=self.fail_on_read, error=self.error
        )
        self.read_position = offset

        def iterator():
            while self.read_position < len(self.data):
                chunk = stream.read(self._chunk_size)
                self.read_position += len(chunk)
                yield chunk

        return iterator()


# pylint: disable=unused-variable
def random_string(length: int = 10) -> str:
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
    bck: BucketModel,
    obj_name: str,
    obj_size: int = 0,
) -> Tuple["Object", bytes]:
    obj_size = obj_size if obj_size else random.randrange(10, 20)
    content = random_string(obj_size).encode(UTF_ENCODING)
    obj = client.bucket(bck.name, provider=bck.provider).object(obj_name)
    obj.get_writer().put_content(content)
    return obj, content


def cleanup_local(path: str):
    try:
        shutil.rmtree(path)
    except FileNotFoundError:
        pass


def cases(*args):
    def decorator(func):
        def wrapper(self, *inner_args, **kwargs):
            for arg in args:
                with self.subTest(arg=arg):
                    func(self, arg, *inner_args, **kwargs)

        return wrapper

    return decorator


def case_matrix(*args_list):
    def decorator(func):
        def wrapper(self, *inner_args, **kwargs):
            for args in product(*args_list):
                with self.subTest(args=args):
                    func(self, *args, *inner_args, **kwargs)

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
    num_files: int, num_extensions: int, min_shard_size: int, dest_dir: Path
):
    def generate_files(
        num_files: int, num_extensions: int, dest_dir: Path
    ) -> Tuple[List[Path], List[str]]:
        files_list = []
        filenames_list = []

        extension_list = [random_string(3) for _ in range(num_extensions)]
        for _ in range(num_files):
            filename = random_string(10)
            filenames_list.append(filename)

            for ext in extension_list:
                file_path = dest_dir.joinpath(f"{filename}.{ext}")
                with open(file_path, "wb") as file:
                    file.write(os.urandom((random.randint(KB, 10 * KB))))
                files_list.append(file_path)

        return files_list, extension_list

    def create_tarballs(
        min_shard_size: int, dest_dir: Path, files_list: List[Path]
    ) -> int:
        num_input_shards = 0
        current_size = 0
        current_tarball = dest_dir.joinpath(f"input-shard-{num_input_shards}.tar")
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
                current_tarball = dest_dir.joinpath(
                    f"input-shard-{num_input_shards}.tar"
                )
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

    file_list, extension_list = generate_files(num_files, num_extensions, dest_dir)
    num_input_shards = create_tarballs(min_shard_size, dest_dir, file_list)
    filename_list = list(map(lambda filepath: filepath.stem, file_list))
    return filename_list, extension_list, num_input_shards


def create_api_error_response(req_url: str, status: str, msg: str) -> requests.Response:
    """
       Given test details, manually generate a requests.Response object

    Args:
        req_url (str): Original request url
        status (str): Response HTTP status code
        msg (str): Response text content

    Returns: requests.Response containing the given details
    """
    req = requests.Request()
    req.url = req_url
    response = requests.Response()
    response.status_code = status
    # pylint: disable=protected-access
    response._content = msg.encode("utf-8")
    response.request = req
    return response


def has_targets(n: int = 2) -> bool:
    """Check if the cluster has at least two targets before running tests."""
    try:
        return len(DEFAULT_TEST_CLIENT.cluster().get_info().tmap) >= n
    except Exception:
        return False  # Assume failure means insufficient targets or unreachable cluster (AuthN)


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.4, max=6),
    retry=retry_if_exception_type(AssertionError),
    reraise=True,
)
def assert_with_retries(
    assertion_fn: Callable[..., None], *args: Any, **kwargs: Any
) -> None:
    assertion_fn(*args, **kwargs)
