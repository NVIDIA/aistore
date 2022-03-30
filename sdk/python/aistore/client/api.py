#
# Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
#

import requests

from .const import (
    URL_PARAM_ARCHPATH,
    URL_PARAM_PROVIDER,
)
from .msg import ActionMsg


# pylint: disable=unused-variable
class Client:
    def __init__(self, url):
        self.url = url
        self._base_url = "{}/{}".format(self.url, "v1")

    @property
    def base_url(self):
        return self._base_url

    def list_buckets(self, provider=""):
        url = "{}/buckets".format(self.base_url)
        params = {URL_PARAM_PROVIDER: provider}
        action = ActionMsg("list").json()
        resp = requests.get(url=url, headers={"Accept": "application/json"}, json=action, params=params)
        return resp.json()

    def create_bucket(self, bck):
        url = "{}/buckets/{}".format(self.base_url, bck.name)
        params = {URL_PARAM_PROVIDER: bck.provider}
        action = ActionMsg("create-bck").json()
        return requests.post(url=url, json=action, params=params)

    def destroy_bucket(self, bck):
        url = "{}/buckets/{}".format(self.base_url, bck.name)
        params = {URL_PARAM_PROVIDER: bck.provider}
        action = ActionMsg("destroy-bck").json()
        return requests.delete(url=url, json=action, params=params)

    def list_objects(self, bck, prefix=""):
        url = "{}/buckets/{}".format(self.base_url, bck.name)

        params = {"action": "list", "value": {}}
        if prefix != "":
            params["value"]["prefix"] = prefix

        resp = requests.get(
            url=url,
            json=params,
            headers={"Accept": "application/json"},
        )
        if resp.status_code == 200:
            entries = resp.json()["entries"]
            return entries

        return resp.json()

    def get_object(self, bck, object_name, transform_id="", archpath=""):
        url = "{}/objects/{}/{}".format(self.base_url, bck.name, object_name)
        params = {}
        if bck.provider != "":
            params[URL_PARAM_PROVIDER] = bck.provider
        if archpath != "":
            params[URL_PARAM_ARCHPATH] = archpath
        if transform_id != "":
            params["uuid"] = transform_id
        return requests.get(url=url, params=params)

    def put_object(self, bck, object_name, path):
        url = "{}/objects/{}/{}".format(self.base_url, bck.name, object_name)
        params = {}
        if bck.provider != "":
            params[URL_PARAM_PROVIDER] = bck.provider
        with open(path, "rb") as data:
            return requests.put(url=url, params=params, data=data)

    def get_cluster_info(self):
        url = "{}/daemon".format(self.base_url)
        return requests.get(url, params={"what": "smap"}).json()
