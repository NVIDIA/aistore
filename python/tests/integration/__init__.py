import os

from aistore.sdk.const import PROVIDER_AIS

CLUSTER_ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
REMOTE_BUCKET = os.environ.get("BUCKET", "")
REMOTE_SET = REMOTE_BUCKET != "" and not REMOTE_BUCKET.startswith(PROVIDER_AIS + ":")
TEST_TIMEOUT = 30
TEST_TIMEOUT_LONG = 120
OBJECT_COUNT = 10
STRESS_TEST_OBJECT_COUNT = 500
