import os
from aistore.sdk.const import AWS_DEFAULT_REGION

AWS_SESSION_TOKEN = os.environ.get("AWS_SESSION_TOKEN", "testing")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", AWS_DEFAULT_REGION)
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY", "testing")
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID", "testing")
AWS_SECURITY_TOKEN = os.environ.get("AWS_SECURITY_TOKEN", "testing")
