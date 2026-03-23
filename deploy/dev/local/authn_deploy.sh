#!/bin/bash

AISTORE_PATH=$(cd "$(dirname "$0")/../../../" || exit; pwd -P)
source "${AISTORE_PATH}/deploy/dev/utils.sh"

APP_CONF_DIR="${APP_CONF_DIR:-$HOME/.config/ais}"
LOG_ROOT="${LOG_ROOT:-/tmp/ais}"

AIS_AUTHN_CONF_DIR="${AIS_AUTHN_CONF_DIR:-${APP_CONF_DIR}/authn}"
AIS_AUTHN_LOG_DIR="${AIS_AUTHN_LOG_DIR:-${LOG_ROOT}/authn/log}"
AIS_AUTHN_SU_NAME="${AIS_AUTHN_SU_NAME:-admin}"

build_authn() {
  if ! make --no-print-directory -C "${AISTORE_PATH}" authn; then
    exit_error "failed to compile 'authn' binary"
  fi
}

generate_authn_conf() {
  echo "Generating authN configuration files in ${AIS_AUTHN_CONF_DIR}..."
  mkdir -p "${AIS_AUTHN_CONF_DIR}"
  source "${AISTORE_PATH}/deploy/dev/local/authn_config.sh"
}

run_authn() {
  echo "Running authN in background with config from ${AIS_AUTHN_CONF_DIR}..."
  "${GOPATH}/bin/authn" "-config=${AIS_AUTHN_CONF_DIR}" &
}

# When executed directly (not sourced), build, configure, and run in foreground
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
  build_authn
  generate_authn_conf
  run_authn
fi
