#
# Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
#

# Standard Header Keys
HEADER_ACCEPT = "Accept"
HEADER_USER_AGENT = "User-Agent"
HEADER_CONTENT_TYPE = "Content-Type"
HEADER_CONTENT_LENGTH = "Content-Length"
HEADER_LOCATION = "Location"
# Standard Header Values
USER_AGENT_BASE = "ais/python"
JSON_CONTENT_TYPE = "application/json"
MSGPACK_CONTENT_TYPE = "application/msgpack"
# AIS Headers
AIS_CHECKSUM_TYPE = "ais-checksum-type"
AIS_CHECKSUM_VALUE = "ais-checksum-value"
AIS_ACCESS_TIME = "ais-atime"
AIS_VERSION = "ais-version"
AIS_CUSTOM_MD = "ais-custom-md"
AIS_BCK_NAME = "ais-bucket-name"
AIS_BCK_PROVIDER = "ais-bucket-provider"
AIS_OBJ_NAME = "ais-name"
AIS_LOCATION = "ais-location"
AIS_MIRROR_PATHS = "ais-mirror-paths"
AIS_MIRROR_COPIES = "ais-mirror-copies"
AIS_PRESENT = "ais-present"
# Bucket Props Header keys
HEADER_PREFIX = "ais-"
HEADER_BUCKET_PROPS = HEADER_PREFIX + "bucket-props"
HEADER_BUCKET_SUMM = HEADER_PREFIX + "bucket-summ"
HEADER_XACTION_ID = HEADER_PREFIX + "xaction-id"
HEADER_NODE_ID = HEADER_PREFIX + "node-id"
# Object Props Header Keys
HEADER_OBJECT_BLOB_DOWNLOAD = HEADER_PREFIX + "blob-download"
HEADER_OBJECT_BLOB_CHUNK_SIZE = HEADER_PREFIX + "blob-chunk"
HEADER_OBJECT_BLOB_WORKERS = HEADER_PREFIX + "blob-workers"
HEADER_OBJECT_APPEND_HANDLE = "ais-append-handle"
# Ref: https://www.rfc-editor.org/rfc/rfc7233#section-2.1
HEADER_RANGE = "Range"
# AuthN Headers
HEADER_AUTHORIZATION = "Authorization"

# URL Params
# See api/apc/query.go
QPARAM_WHAT = "what"
QPARAM_ETL_NAME = "etl_name"
QPARAM_PROVIDER = "provider"
QPARAM_BCK_TO = "bck_to"
QPARAM_FLT_PRESENCE = "presence"
QPARAM_BSUMM_REMOTE = "bsumm_remote"
QPARAM_KEEP_REMOTE = "keep_bck_md"
QPARAM_ARCHPATH = "archpath"
QPARAM_ARCHREGX = "archregx"
QPARAM_ARCHMODE = "archmode"
QPARAM_FORCE = "frc"
QPARAM_PRIMARY_READY_REB = "prr"
QPARAM_NAMESPACE = "namespace"
QPARAM_OBJ_APPEND = "append_type"
QPARAM_OBJ_APPEND_HANDLE = "append_handle"
DSORT_UUID = "uuid"
QPARAM_UUID = "uuid"
QPARAM_LATEST = "latest-ver"
QPARAM_NEW_CUSTOM = "set-new-custom"

# URL Param values
# See api/apc/query.go
WHAT_SMAP = "smap"
WHAT_ONE_XACT_STATUS = "status"
WHAT_ALL_XACT_STATUS = "status_all"
WHAT_ALL_RUNNING_STATUS = "running_all"
WHAT_QUERY_XACT_STATS = "qryxstats"
WHAT_NODE_STATS_AND_STATUS_V322 = "status"
WHAT_NODE_STATS_AND_STATUS = "node_status"

# URL paths
URL_PATH_CLUSTER = "cluster"
URL_PATH_BUCKETS = "buckets"
URL_PATH_OBJECTS = "objects"
URL_PATH_HEALTH = "health"
URL_PATH_DAEMON = "daemon"
URL_PATH_ETL = "etl"
URL_PATH_DSORT = "sort"
URL_PATH_REVERSE = "reverse"
DSORT_ABORT = "abort"
# AuthN
URL_PATH_AUTHN_USERS = "users"
URL_PATH_AUTHN_CLUSTERS = "clusters"
URL_PATH_AUTHN_ROLES = "roles"
URL_PATH_AUTHN_TOKENS = "tokens"

# HTTP Methods
HTTP_METHOD_GET = "get"
HTTP_METHOD_POST = "post"
HTTP_METHOD_DELETE = "delete"
HTTP_METHOD_PUT = "put"
HTTP_METHOD_HEAD = "head"
HTTP_METHOD_PATCH = "patch"

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
ACT_SUMMARY_BCK = "summary-bck"
ACT_BLOB_DOWNLOAD = "blob-download"
# Multi-object actions
ACT_DELETE_OBJECTS = "delete-listrange"
ACT_EVICT_OBJECTS = "evict-listrange"
ACT_PREFETCH_OBJECTS = "prefetch-listrange"
ACT_COPY_OBJECTS = "copy-listrange"
ACT_TRANSFORM_OBJECTS = "etl-listrange"
ACT_ARCHIVE_OBJECTS = "archive"
# Job actions
ACT_START = "start"

# Defaults
DEFAULT_CHUNK_SIZE = 32768
DEFAULT_JOB_WAIT_TIMEOUT = 300
DEFAULT_DSORT_WAIT_TIMEOUT = 300
DEFAULT_DATASET_MAX_COUNT = 100000
DEFAULT_JOB_POLL_TIME = 0.2

# ENCODING
UTF_ENCODING = "utf-8"

# Status Codes
STATUS_ACCEPTED = 202
STATUS_OK = 200
STATUS_BAD_REQUEST = 400
STATUS_PARTIAL_CONTENT = 206
STATUS_REDIRECT_TMP = 307
STATUS_REDIRECT_PERM = 301

# Protocol
HTTP = "http://"
HTTPS = "https://"

# Environment Variables
AIS_CLIENT_CA = "AIS_CLIENT_CA"
AIS_AUTHN_TOKEN = "AIS_AUTHN_TOKEN"
AIS_CLIENT_CRT = "AIS_CRT"
AIS_CLIENT_KEY = "AIS_CRT_KEY"

# Content Constants
LOREM = (
    "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do eiusmod"
    " tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim"
    " veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea"
    " commodo consequat."
)
DUIS = (
    "Duis aute irure dolor in reprehenderit in voluptate velit esse cillum"
    " dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non"
    " proident, sunt in culpa qui officia deserunt mollit anim id est laborum."
    " Et harum quidem.."
)

# AWS Constants
AWS_DEFAULT_REGION = "us-east-1"

# Time constants
NANOSECONDS_IN_SECOND = 1_000_000_000

DEFAULT_LOG_FORMAT = "%(asctime)s %(levelname)s: %(message)s"
