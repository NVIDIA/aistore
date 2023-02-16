#
# Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
#

# pylint: disable=unused-variable,invalid-name

# URL Params
QParamArchpath = "archpath"
QParamProvider = "provider"
QParamWhat = "what"
QParamKeepBckMD = "keep_md"
QParamBucketTo = "bck_to"
QparamPrimaryReadyReb = "prr"
QParamETLName = "etl_name"
QParamForce = "frc"

# URL Param values
QParamSmap = "smap"
QParamStatus = "status"

# URL paths
URL_PATH_CLUSTER = "cluster"
URL_PATH_BUCKETS = "buckets"
URL_PATH_OBJECTS = "objects"
URL_PATH_HEALTH = "health"
URL_PATH_DAEMON = "daemon"
URL_PATH_ETL = "etl"

# Bucket providers
ProviderAIS = "ais"
ProviderAmazon = "aws"
ProviderGoogle = "gcp"
ProviderHTTP = "ht"
ProviderAzure = "azure"
ProviderHDFS = "hdfs"

HTTP_METHOD_GET = "get"
HTTP_METHOD_POST = "post"
HTTP_METHOD_DELETE = "delete"
HTTP_METHOD_PUT = "put"
HTTP_METHOD_HEAD = "head"

# Actions
ACT_COPY_BCK = "copy-bck"
ACT_CREATE_BCK = "create-bck"
ACT_DESTROY_BCK = "destroy-bck"
ACT_EVICT_REMOTE_BCK = "evict-remote-bck"
ACT_LIST = "list"
ACT_PROMOTE = "promote"
ACT_MOVE_BCK = "move-bck"
ACT_ETL_BCK = "etl-bck"
ACT_DELETE_MULTIPLE_OBJ = "delete-listrange"
ACT_EVICT_MULTIPLE_OBJ = "evict-listrange"
ACT_PREFETCH_MULTIPLE_OBJ = "prefetch-listrange"
ACT_START = "start"

# Headers
CONTENT_LENGTH = "content-length"
AIS_CHECKSUM_TYPE = "ais-checksum-type"
AIS_CHECKSUM_VALUE = "ais-checksum-value"
AIS_ACCESS_TIME = "ais-atime"
AIS_VERSION = "ais-version"
AIS_CUSTOM_MD = "ais-custom-md"

# Defaults
DEFAULT_CHUNK_SIZE = 32768
DEFAULT_JOB_WAIT_TIMEOUT = 300
DEFAULT_ETL_COMM = "hpush"
DEFAULT_ETL_TIMEOUT = "5m"

# ENCODING
UTF_ENCODING = "utf-8"

# templates for ETL

CODE_TEMPLATE = """
import pickle
import base64

transform = pickle.loads(base64.b64decode('{}'))
{}
"""
