import random
import string
import tempfile

from aistore.client.api import Client

# pylint: disable=unused-variable
def random_name(length: int = 10):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


# pylint: disable=unused-variable
def create_and_put_object(
    client: Client, bck_name: str, obj_name: str, provider: str = "ais"
):
    object_body = "test string" * random.randrange(1, 10)
    content = object_body.encode("utf-8")
    # obj_name = "temp/obj1"
    with tempfile.NamedTemporaryFile() as file:
        file.write(content)
        file.flush()
        client.bucket(bck_name, provider=provider).object(obj_name).put(file.name)
    return content
