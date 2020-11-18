#!/bin/bash

############################################
#
# Usage: clean.sh
#
############################################

AISTORE_DIR=$(cd "$(dirname "$0")/../../../"; pwd -P) # absolute path to aistore directory
source $AISTORE_DIR/deploy/dev/utils.sh

# Unmount and clears all virtual block disks.
clean_loopback_paths

rm -rf ~/.ais* && \
		rm -rf ~/.authn && \
		rm -rf /tmp/ais* && \
		rm -f ${BUILD_DEST}/ais* # cleans 'ais' (CLI), 'aisnode' (TARGET/PROXY), 'aisfs' (FUSE), 'aisloader' && \
		rm -f ${BUILD_DEST}/authn
