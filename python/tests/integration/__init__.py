import os

from aistore.sdk.provider import Provider, ALIAS_S3

CLUSTER_ENDPOINT = os.environ.get("AIS_ENDPOINT", "http://localhost:8080")
REMOTE_BUCKET = os.environ.get("BUCKET", "")
REMOTE_SET = REMOTE_BUCKET and not REMOTE_BUCKET.startswith(Provider.AIS.value + ":")
AWS_BUCKET = (
    REMOTE_SET
    and REMOTE_BUCKET.startswith(Provider.AMAZON.value + ":")
    or REMOTE_BUCKET.startswith(ALIAS_S3 + ":")
)

# AuthN
AUTHN_ENDPOINT = os.environ.get("AIS_AUTHN_URL", "http://localhost:52001")
AIS_AUTHN_SU_NAME = os.environ.get("AIS_AUTHN_SU_NAME", "admin")
AIS_AUTHN_SU_PASS = os.environ.get("AIS_AUTHN_SU_PASS", "admin")
