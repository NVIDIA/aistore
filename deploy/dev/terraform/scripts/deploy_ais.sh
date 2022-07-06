#!/bin/bash

. /etc/profile.d/aispaths.sh

cd "${AISTORE_SRC}"
echo "Deploying AIStore: ${AIS_BACKEND_PROVIDERS}"
make kill deploy <<< $'1\n1\n1\nn\nn\nn\nn\n0\n'
make cli
