import random
import string
import tempfile

from aistore.client.api import Client

# pylint: disable=unused-variable
def random_name(length: int = 10):
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
    content = obj_body.encode("utf-8")
    with tempfile.NamedTemporaryFile() as file:
        file.write(content)
        file.flush()
        client.bucket(bck_name, provider=provider).object(obj_name).put(file.name)
    return content
