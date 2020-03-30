#
# Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
#

import requests, json

TAR2TF = "tar2tf"
OBJECTS = "objects"
START = "start"


# pylint: disable=unused-variable
class AisClient:
    def __init__(self, url, bucket):
        self.url = url
        self.bucket = bucket

    def __get_base_url(self):
        return "{}/{}".format(self.url, "v1")

    def get_object(self, object_name):
        url = "{}/objects/{}/{}".format(self.__get_base_url(), self.bucket, object_name)
        return requests.get(url=url).content

    def get_cluster_info(self):
        url = "{}/daemon".format(self.__get_base_url())
        return requests.get(url, params={"what": "smap"}).json()

    def get_objects_names(self, target_url, template):
        url = "{}/v1/{}/{}/{}/{}".format(target_url, TAR2TF, OBJECTS, self.bucket, template)
        return requests.get(url=url)

    def start_target_job_stream(self, target_url, target_msg):
        url = "{}/v1/{}/{}/{}".format(target_url, TAR2TF, START, self.bucket)
        return requests.get(url=url, data=json.dumps(dict(target_msg)), stream=True)
