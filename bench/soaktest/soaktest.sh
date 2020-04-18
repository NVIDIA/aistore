#!/bin/bash


SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TMP_DIR=/tmp/ais-soak/soaktestexec
OUT_FIL="${TMP_DIR}/soaktest"

cd "${SCRIPT_DIR}"
rm -rf "${TMP_DIR}"
mkdir -p "${TMP_DIR}"
go build -o "${OUT_FIL}"

export START_CMD="soaktest.sh"  #indicates that soaktest was run from the bash script

"${OUT_FIL}" ${@}
