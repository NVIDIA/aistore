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

rm -rf ~/.ais*            # cluster config and metadata
rm -rf ~/.config/ais      # CLI and AuthN config and token, respectively
rm -rf /tmp/ais*          # user data and cluster metadata
rm -f ${build_dest}/ais*  # 'ais' (CLI), 'aisnode', 'aisfs', and 'aisloader' binaries
rm -f ${build_dest}/authn # AuthN executable
