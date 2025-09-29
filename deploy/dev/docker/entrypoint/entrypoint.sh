#!/bin/bash

# required for `source /aisnode_config.sh`
export TEST_FSPATH_ROOT=${MOUNTPATH}/${HOSTNAME}
export AIS_LOG_DIR=${TEST_FSPATH_ROOT}/log
mkdir -p ${AIS_CONF_DIR}
mkdir -p ${TEST_FSPATH_ROOT}
mkdir -p ${AIS_LOG_DIR}
export GOCACHE=/tmp/.gocache
mkdir -p /tmp/.gocache
source /utils.sh
source /aisnode_config.sh

${GOBIN}/aisnode \
    -config=${AIS_CONF_FILE} \
    -local_config=${AIS_LOCAL_CONF_FILE} \
    -role=${AIS_NODE_ROLE} \
    -ntargets=${TARGET_CNT} \
    -nodiskio=${AIS_NO_DISK_IO} \
    -dryobjsize=${AIS_DRY_OBJ_SIZE}
