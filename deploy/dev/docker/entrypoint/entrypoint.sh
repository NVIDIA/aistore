#!/bin/bash

# required for `source /aisnode_config.sh`
export TEST_FSPATH_ROOT=${MOUNTPATH}/${HOSTNAME}
export AIS_LOG_DIR=${TEST_FSPATH_ROOT}/log
mkdir -p ${AIS_CONF_DIR}
mkdir -p ${TEST_FSPATH_ROOT}
mkdir -p ${AIS_LOG_DIR}
export GOCACHE=/tmp/.gocache
mkdir -p /tmp/.gocache
touch ${AIS_LOG_DIR}/statsd.log
source /aisnode_config.sh

exec node /statsd/stats.js ${STATSD_CONF_FILE} 2>&1 | tee -a ${AIS_LOG_DIR}/statsd.log &
cd ${GOPATH}/src/github.com/NVIDIA/aistore
make node
AIS_DAEMON_ID=$(echo ${HOSTNAME}) ${GOBIN}/aisnode \
    -config=${AIS_CONF_FILE} \
    -role=${AIS_NODE_ROLE} \
    -ntargets=${TARGET_CNT} \
    -nodiskio=${AIS_NO_DISK_IO} \
    -dryobjsize=${AIS_DRY_OBJ_SIZE} \
    -alsologtostderr=true
