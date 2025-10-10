#
# Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
#
from tests import IS_STRESS

# SI and IEC Units
KB = 10**3
MB = 10**6
GB = 10**9
KIB = 2**10
MIB = 2**20
GIB = 2**30

# Object Sizes
LARGE_FILE_SIZE = 20 * MIB
MEDIUM_FILE_SIZE = 2 * MIB
SMALL_FILE_SIZE = 10 * KIB

# ETL
ETL_NAME = "test-etl-name"

# Object
OBJ_READ_TYPE_ALL = "read_all"
OBJ_READ_TYPE_CHUNK = "chunk"
OBJ_CONTENT = "test-content"
OBJECT_COUNT = 500 if IS_STRESS else 10

# Timeout
TEST_TIMEOUT = 30
TEST_TIMEOUT_LONG = 120

# Names
PREFIX_NAME = "prefix-"
SUFFIX_NAME = "-suffix.ext"
