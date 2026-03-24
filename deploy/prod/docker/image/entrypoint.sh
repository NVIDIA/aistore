#!/bin/bash
#
# Per-role entrypoint for compose-based AIS deployment.
# Usage: entrypoint.sh <proxy|target> <port>
#

set -e

ROLE=${1:?usage: entrypoint.sh <proxy|target> <port>}
PORT=${2:?usage: entrypoint.sh <proxy|target> <port>}

export AIS_CONF_DIR=/etc/aisnode/${ROLE}
export AIS_CONF_FILE=${AIS_CONF_DIR}/ais.json
export AIS_LOCAL_CONF_FILE=${AIS_CONF_DIR}/ais_local.json
export PORT
export AIS_LOG_DIR=/var/log/aisnode/${ROLE}
export AIS_PRIMARY_URL=${AIS_PRIMARY_URL:?AIS_PRIMARY_URL must be set}

# Advertise using the container hostname (compose DNS name) so that
# the proxy's URLs match AIS_PRIMARY_URL and inter-node routing works.
export HOSTNAME_LIST=${HOSTNAME_LIST:-$(hostname)}
export HOSTNAME_LIST_INTRA_CONTROL=${HOSTNAME_LIST_INTRA_CONTROL:-${HOSTNAME_LIST}}
export HOSTNAME_LIST_INTRA_DATA=${HOSTNAME_LIST_INTRA_DATA:-${HOSTNAME_LIST}}

if [[ "${ROLE}" == "target" ]]; then
  export AIS_FS_PATHS=${AIS_FS_PATHS:-$(ls -d /ais/* 2>/dev/null | while read x; do echo -e "\"$x\": \"${AIS_FS_LABEL:-}\""; done | paste -sd ",")}
fi

mkdir -p ${AIS_CONF_DIR} ${AIS_LOG_DIR}

cd /build
source utils.sh
source aisnode_config.sh

exec bin/aisnode \
  -config=${AIS_CONF_FILE} \
  -local_config=${AIS_LOCAL_CONF_FILE} \
  -role=${ROLE} \
  -ntargets=${AIS_NTARGETS:-1}
