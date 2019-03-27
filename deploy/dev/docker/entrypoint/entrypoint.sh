#!/bin/bash

# required for `source /config.sh`
export TEST_FSPATH_ROOT=${MOUNTPATH}/${HOSTNAME}
export LOGDIR=${TEST_FSPATH_ROOT}/log
mkdir -p ${CONFDIR}
mkdir -p ${TEST_FSPATH_ROOT}
mkdir -p ${LOGDIR}
export GOCACHE=/tmp/.gocache
touch ${LOGDIR}/statsd.log

if [ -n "${QUICK}" ]; then
    go get -u -v github.com/NVIDIA/aistore/ais && /bin/bash
else
    cd ${GOPATH}/src/github.com/NVIDIA/aistore/ais
    source /config.sh

    exec node /statsd/stats.js ${CONFFILE_STATSD} 2>&1 | tee -a ${LOGDIR}/statsd.log &

    VERSION=`git rev-parse --short HEAD`
    BUILD=`date +%FT%T%z`
    go install -tags="${CLDPROVIDER}" -ldflags "-w -s -X 'main.version=${VERSION}' -X 'main.build=${BUILD}'" setup/aisnode.go

    AIS_DAEMONID=`echo ${HOSTNAME}` ${GOBIN}/aisnode \
        -config=${CONFFILE} \
        -role=${ROLE} \
        -ntargets=${TARGET_CNT} \
        -nodiskio=${NODISKIO} \
        -nonetio=${NONETIO} \
        -dryobjsize=${DRYOBJSIZE} \
        -alsologtostderr=true
fi
