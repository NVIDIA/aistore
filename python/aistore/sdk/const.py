#
# Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
#

# URL Params
# See api/apc/query.go
QPARAM_WHAT = "what"
QPARAM_ETL_NAME = "etl_name"
QPARAM_PROVIDER = "provider"
QPARAM_BCK_TO = "bck_to"
QPARAM_KEEP_REMOTE = "keep_bck_md"
QPARAM_ARCHPATH = "archpath"
QPARAM_FORCE = "frc"
QPARAM_PRIMARY_READY_REB = "prr"
QPARAM_NAMESPACE = "namespace"

# URL Param values
# See api/apc/query.go
WHAT_SMAP = "smap"
WHAT_ONE_XACT_STATUS = "status"
WHAT_ALL_XACT_STATUS = "status_all"
WHAT_ALL_RUNNING_STATUS = "running_all"
WHAT_QUERY_XACT_STATS = "qryxstats"

# URL paths
URL_PATH_CLUSTER = "cluster"
URL_PATH_BUCKETS = "buckets"
URL_PATH_OBJECTS = "objects"
URL_PATH_HEALTH = "health"
URL_PATH_DAEMON = "daemon"
URL_PATH_ETL = "etl"

# Bucket providers
# See api/apc/provider.go
PROVIDER_AIS = "ais"
PROVIDER_AMAZON = "aws"
PROVIDER_AZURE = "azure"
PROVIDER_GOOGLE = "gcp"
PROVIDER_HDFS = "hdfs"
PROVIDER_HTTP = "ht"

# HTTP Methods
HTTP_METHOD_GET = "get"
HTTP_METHOD_POST = "post"
HTTP_METHOD_DELETE = "delete"
HTTP_METHOD_PUT = "put"
HTTP_METHOD_HEAD = "head"

# Actions
# See api/apc/actmsg.go
ACT_CREATE_BCK = "create-bck"
ACT_DESTROY_BCK = "destroy-bck"
ACT_COPY_BCK = "copy-bck"
ACT_ETL_BCK = "etl-bck"
ACT_EVICT_REMOTE_BCK = "evict-remote-bck"
ACT_LIST = "list"
ACT_MOVE_BCK = "move-bck"
ACT_PROMOTE = "promote"
# Multi-object actions
ACT_DELETE_OBJECTS = "delete-listrange"
ACT_EVICT_OBJECTS = "evict-listrange"
ACT_PREFETCH_OBJECTS = "prefetch-listrange"
ACT_COPY_OBJECTS = "copy-listrange"
ACT_TRANSFORM_OBJECTS = "etl-listrange"
ACT_ARCHIVE_OBJECTS = "archive"
# Job actions
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

# ENCODING
UTF_ENCODING = "utf-8"
