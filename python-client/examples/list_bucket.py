# coding: utf-8

# Bucket list example: prints out all the bucket objects (names and sizes)
# page by page. After each page it prints page summary.
#
# Bucket name is set via environment variable BUCKET. Example:
#  $ cd python-client
#  $ BUCKET=nvais python list_bucket.py
#  Page #1, objects 2, marker:
#    imageset/tiny-set.tar              32 KB
#    imageset/small-set.tar             1024 KB
#  Done!

# pylint: disable=unused-variable
from __future__ import absolute_import, print_function

# to import generated ais_client package that is not in PYTHONPATH yet
import sys, os, time
sys.path.insert(0, '.')

import ais_client
from ais_client.api.bucket_api import BucketApi  # noqa: E501


# Gets the page and prints all its objects - an object per line.  And it prints
# page summary after the object list.
# Prints 'Done!' if it is the last page.
def print_page(page, num):
    for obj in page["entries"]:
        print("    %-30s %d KB" % (obj["name"], obj["size"] / 1024))
    print("Page #%d, objects %d, marker: %s" % (num, len(page["entries"]), page["pagemarker"]))
    if page["pagemarker"] == "":
        print("Done!")


# Reads the next bucket page. Argument 'marker' defines the last object
# in the previous bucket page
def get_bucket_page(bucket_name, marker):
    requestParams = ais_client.models.ObjectPropertiesRequestParams()
    # request only object sizes
    requestParams.props = ais_client.models.ObjectPropertyTypes.SIZE
    requestParams.pagemarker = marker

    input_params = ais_client.models.InputParameters(ais_client.models.Actions.LISTOBJECTS, value=requestParams)
    bapi = BucketApi()

    # list bucket API is asynchronous, so the first request must return a task
    # ID to monitor its state. Task ID is an int64 number
    resp = bapi.perform_operation(bucket_name, input_params)
    if type(resp) is int:
        # setting request parameter 'taskid' means a server must return state
        # or result of a existing asynchronous bucket list request.
        # If 'taskid' is empty the server starts new bucket list task.
        requestParams.taskid = str(resp)
    else:
        raise "First response must be task ID"

    # the next requests respond either with task ID (if the list is not ready)
    # or with object list (otherwise)
    while True:
        input_params = ais_client.models.InputParameters(ais_client.models.Actions.LISTOBJECTS, value=requestParams)
        # 'int' is task ID, object list is 'dict'
        if not (type(resp) is int):
            return resp

        # sleep for a short while to avoid flooding the server with requests
        # NOTE: for small buckets(or for small pages) the result is ready almost
        # immediately after the task is started. That is why two first requests:
        # to start task and to check its state are done without a break. And only
        # if the second call does not return object list, it is safe to think
        # that it may take some time and it is good to make breaks between calls
        time.sleep(1)
        resp = bapi.perform_operation(bucket_name, input_params)


# read the entire object list page by page (default page size is 1000 objects)
def list_bucket(bucket_name):
    marker = ""
    page_num = 1

    while True:
        page = get_bucket_page(bucket_name, marker)
        print_page(page, page_num)
        # empty 'pagemarker' means it is the last page
        if page["pagemarker"] == "":
            return
        page_num = page_num + 1
        marker = page["pagemarker"]


if __name__ == '__main__':
    bucket = os.environ["BUCKET"]
    list_bucket(bucket)
