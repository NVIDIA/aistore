#!/bin/bash

############################################
#
# Usage: clean.sh
#
############################################

source "$(dirname "$0")/../utils.sh"

# Unmount and clears all virtual block disks.
NEXT_TIER=
rm_loopbacks
NEXT_TIER="_next"
rm_loopbacks

build_dest="${GOPATH}/bin"
if [[ -n ${GOBIN} ]]; then
	build_dest="${GOBIN}"
fi

rm -rf ~/.ais*            # cluster config and metadata
rm -rf ~/.config/ais      # CLI and AuthN config and token, respectively
rm -rf /tmp/ais*          # user data and cluster metadata
rm -f ${build_dest}/ais*  # 'ais' (CLI), 'aisnode', 'aisfs', and 'aisloader' binaries
rm -f ${build_dest}/authn # AuthN executable
