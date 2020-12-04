#!/bin/bash

############################################
#
# Usage: clean.sh
#
############################################

source "$(dirname "$0")/../utils.sh"

# Unmount and clears all virtual block disks.
clean_loopback_paths

build_dest="${GOPATH}/bin"
if [[ -n ${GOBIN} ]]; then
	build_dest="${GOBIN}"
fi

rm -rf ~/.ais* && \
		rm -rf ~/.authn && \
		rm -rf /tmp/ais* && \
		rm -f ${build_dest}/ais* # cleans 'ais' (CLI), 'aisnode' (TARGET/PROXY), 'aisfs' (FUSE), 'aisloader' && \
		rm -f ${build_dest}/authn
