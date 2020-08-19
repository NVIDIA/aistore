#!/bin/bash

set -e

. /etc/profile.d/aispaths.sh

echo "Pulling latest AIStore source..."
cd ${AISTORE_SRC}
git fetch
git reset --hard origin/master
git status
if [[ -n $1 ]]; then
    echo Git checkout branch $1
    git checkout $1
fi

echo "Cloud provider(s) set to: ${AIS_CLD_PROVIDERS}"
make node
