#!/bin/bash

# required for `source /aisnode_config.sh`
export TEST_FSPATH_ROOT=${MOUNTPATH}/${HOSTNAME}
export LOGDIR=${TEST_FSPATH_ROOT}/log
mkdir -p ${CONFDIR}
mkdir -p ${TEST_FSPATH_ROOT}
mkdir -p ${LOGDIR}
export GOCACHE=/tmp/.gocache
touch ${LOGDIR}/statsd.log

if [ -n "${QUICK}" ]; then
  go get -u -v github.com/NVIDIA/aistore && /bin/bash
else
  cd ${GOPATH}/src/github.com/NVIDIA/aistore
  source /aisnode_config.sh

  exec node /statsd/stats.js ${CONFFILE_STATSD} 2>&1 | tee -a ${LOGDIR}/statsd.log &

  make node
  AIS_DAEMONID=$(echo ${HOSTNAME}) ${GOBIN}/aisnode \
      -config=${CONFFILE} \
      -role=${ROLE} \
      -ntargets=${TARGET_CNT} \
      -nodiskio=${NODISKIO} \
      -dryobjsize=${DRYOBJSIZE} \
      -alsologtostderr=true
fi
