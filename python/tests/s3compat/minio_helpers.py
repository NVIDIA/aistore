#
# The functions in this file are modified under the Apache 2.0 license as provided by minio
# The original code can be found at https://github.com/minio/minio-py
#
# MinIO Python Library for Amazon S3 Compatible Cloud Storage,
# (C) 2015, 2016, 2017, 2018 MinIO, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import shutil
import time
import traceback
from inspect import getfullargspec
from minio import Minio
from minio.error import S3Error
from tests import LimitedRandomReader, MB
import tests


class TestFailed(Exception):
    """Indicate test failed error."""


# pylint: disable=unused-variable,protected-access
def init_tests(server_endpoint, access_key, secret_key):
    tests._CLIENT = Minio(server_endpoint, access_key, secret_key, secure=False)
    test_file = "datafile-1-MB"
    large_file = "datafile-11-MB"
    with open(test_file, "wb") as file_data:
        shutil.copyfileobj(LimitedRandomReader(1 * MB), file_data)
    with open(large_file, "wb") as file_data:
        shutil.copyfileobj(LimitedRandomReader(11 * MB), file_data)
    tests._TEST_FILE = test_file
    tests._LARGE_FILE = large_file


def call_test(func, strict):
    """Execute given test function."""

    log_entry = {
        "name": func.__name__,
        "status": "PASS",
    }

    start_time = time.time()
    try:
        func(log_entry)
    except S3Error as exc:
        if exc.code == "NotImplemented":
            log_entry["alert"] = "Not Implemented"
            log_entry["status"] = "NA"
        else:
            log_entry["message"] = f"{exc}"
            log_entry["error"] = traceback.format_exc()
            log_entry["status"] = "FAIL"
    except Exception as exc:  # pylint: disable=broad-except
        log_entry["message"] = f"{exc}"
        log_entry["error"] = traceback.format_exc()
        log_entry["status"] = "FAIL"

    if log_entry.get("method"):
        # pylint: disable=deprecated-method
        args_string = ", ".join(getfullargspec(log_entry["method"]).args[1:])
        log_entry["function"] = f"{log_entry['method'].__name__}({args_string})"
    log_entry["args"] = {k: v for k, v in log_entry.get("args", {}).items() if v}
    log_entry["duration"] = int(round((time.time() - start_time) * 1000))
    log_entry["name"] = "minio-py:" + log_entry["name"]
    log_entry["method"] = None
    print(json.dumps({k: v for k, v in log_entry.items() if v}))
    print()
    if strict and log_entry["status"] == "FAIL":
        raise TestFailed()
