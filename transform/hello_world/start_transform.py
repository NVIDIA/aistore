import json
import requests


class AisClient:
    def __init__(self, url, bucket):
        self.url = url
        self.bucket = bucket

    def __get_base_url(self):
        return "{}/{}".format(self.url, "v1")

    def transform_init(self, spec):
        url = "{}/transform/init".format(self.__get_base_url())
        return requests.get(url=url, data=json.dumps(spec)).content

    def transform_object(self, transform_id, object_name):
        url = "{}/objects/{}/{}?uuid={}".format(
            self.__get_base_url(),
            self.bucket,
            object_name,
            transform_id,
        )
        return requests.get(url=url).content


def main():
    client = AisClient(url="http://localhost:31337", bucket="tmp")

    # Initialize transform
    f = open('hello_world.yaml', 'r')
    spec = f.read()
    transform_id = client.transform_init(spec=spec)

    # Transform objects
    for i in range(0, 10):
        object_name = "shard-{}.tar".format(i)
        output = client.transform_object(
            transform_id=transform_id,
            object_name=object_name,
        )
        print(f"{object_name} -> {output}")


if __name__ == '__main__':
    main()
