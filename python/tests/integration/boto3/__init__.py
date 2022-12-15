import os

NUM_BUCKETS = os.environ.get("BOTOTEST_NUM_BUCKETS", 10)
NUM_OBJECTS = os.environ.get("BOTOTEST_NUM_OBJECTS", 1000)
OBJECT_LENGTH = os.environ.get("BOTOTEST_OBJECT_LENGTH", 10000)
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
