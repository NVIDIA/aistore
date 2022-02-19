#!/bin/bash

run_tests() {
  SECONDS=0

  if [[ -n "${RE}" ]]; then
    re="-run=${RE}"
  fi

  tests_dir="${AISTORE_DIR}/..."
  if [[ -n "${TESTS_DIR}" ]]; then
    tests_dir="${AISTORE_DIR}/${TESTS_DIR}"
  fi

  timeout="-timeout=2h"
  if [[ -n "${SHORT}" ]]; then
    short="-short"
    shuffle="-shuffle=on"
    timeout="-timeout=30m"
  fi

  failed_tests=$(
    BUCKET="${BUCKET}" AIS_ENDPOINT="${AIS_ENDPOINT}" \
      go test -v -p 1 -tags debug -parallel 4 -count 1 ${timeout} ${short} ${shuffle} ${re} "${tests_dir}" 2>&1 \
    | tee -a /dev/stderr \
    | grep -ae "^---FAIL: Bench\|^--- FAIL: Test\|^FAIL[[:space:]]github.com/NVIDIA/.*$"; \
    exit ${PIPESTATUS[0]} # Exit with the status of the first command in the pipe(line).
  )
  exit_code=$?

  echo "Tests took: $((SECONDS/3600))h$(((SECONDS%3600)/60))m$((SECONDS%60))s"

  if [[ $exit_code -ne 0 ]]; then
    echo "${failed_tests}"
    exit $exit_code
  fi
}

AISTORE_DIR="$(cd "$(dirname "$0")/../../"; pwd -P)"
YAPF_STYLE="$(dirname ${0})/config/.style.yapf"
PYLINT_STYLE="$(dirname ${0})/config/.pylintrc"
EXTERNAL_SRC_REGEX=".*\(venv\|build\|3rdparty\|dist\|.idea\|.vscode\)/.*"
# This script is used by Makefile to run commands.
source ${AISTORE_DIR}/deploy/scripts/utils.sh

case $1 in
lint)
  echo "Running lint..." >&2
  golangci-lint run $(list_all_go_dirs)
  exit $?
  ;;

fmt)
  err_count=0
  case $2 in
  --fix)
    echo "Running style fixing..." >&2

    gofumpt -s -w ${AISTORE_DIR}
    python_yapf_fix
    ;;
  *)
    echo "Running style check..." >&2

    out=$(gofmt -l -e ${AISTORE_DIR})

    if [[ -n ${out} ]]; then
      echo ${out} >&2
      exit 1
    fi

    check_gomod
    check_imports
    check_deps
    check_files_headers
    check_python_formatting
    ;;
  esac
  ;;

spell)
  echo "Running spell check..." >&2
  case $2 in
  --fix)
    ${GOPATH}/bin/misspell -i "importas" -w -locale=US ${AISTORE_DIR}
    ;;
  *)
    ${GOPATH}/bin/misspell -i "importas" -error -locale=US ${AISTORE_DIR}
    ;;
  esac
  ;;

test-env)
  if [[ -z ${BUCKET} ]]; then
    echo "Error: missing environment variable: BUCKET=\"bucketname\""
    exit 1
  fi

  hash docker &>/dev/null
  if [[ $? -eq 0 ]]; then
    docker_running=$(docker container ls)
    if [[ $? -ne 0 ]]; then
      echo "Warning: Can't check if AIS is running from docker, verify that you have permissions for /var/run/docker.sock" >&2
    elif [[ $(echo ${docker_running} | grep ais) ]]; then
      echo "AIStore running on docker..." >&2
      exit 0
    fi
  fi

  if [[ -n ${KUBERNETES_SERVICE_HOST} ]]; then
    echo "AIStore running on Kubernetes..." >&2
    exit 0
  fi

  if [[ -n $(pgrep aisnode) ]]; then
    echo "AIStore running locally..." >&2
    exit 0
  fi

  if curl -s -X GET "${AIS_ENDPOINT}/v1/health?readiness=true"; then
    echo "AIStore detected on $AIS_ENDPOINT"
    exit 0
  fi

  echo "AIStore is not running, this causes some tests to fail! (to run, see: https://github.com/NVIDIA/aistore#local-non-containerized)" >&2
  echo -n "continue? [y/N] " >&2 && read ans && [[ ${ans:-N} =~ ^y(es)?$ ]]
  exit $?
  ;;

test-short)
  echo "Running short tests..." >&2
  SHORT="true" run_tests
  ;;

test-long)
  echo "Running long tests..." >&2
  run_tests
  ;;

test-run)
  echo "Running test with regex (${RE})..." >&2
  run_tests
  ;;

test-docker)
  docker_state=$(docker info >/dev/null 2>&1)
  if [[ $? -ne 0 ]]; then
    echo "Docker does not seem to be running, run it first and retry."
    exit 1
  fi

  echo "Running test in Docker..." >&2
  branch=$(git branch | grep \* | cut -d ' ' -f2)
  errs=$("${AISTORE_DIR}/deploy/test/docker/test.sh" --name=${branch} 2>&1 | tee -a /dev/stderr | grep -e "^--- FAIL: Bench\|^--- FAIL: Test"  )
  perror $1 "${errs}"
  ;;


test-bench)
  echo "Running benchmark tests..." >&2
  SCRIPT_ROOT=${AISTORE_DIR}/deploy/scripts
  . ${SCRIPT_ROOT}/bench.sh cmp --dir "${AISTORE_DIR}" --verbose --post-checkout "${SCRIPT_ROOT}/clean_deploy.sh --dir ${AISTORE_DIR}"
  ;;

bench)
  shift
  . ${AISTORE_DIR}/deploy/scripts/bench.sh
  ;;

clean-deploy)
  shift
  . ${AISTORE_DIR}/deploy/scripts/clean_deploy.sh
  ;;

dev-init)
  if [[ -z ${REMOTE} ]]; then
    echo "Missing environment variable: REMOTE=\"http://path/to/remote\""
    exit
  fi

  if [[ -z $(command -v git) ]]; then
    echo "'git' command not installed"
    exit 1
  elif [[ -z $(git remote -v | grep origin) ]]; then
    git remote add origin "${REMOTE}";
  else
    git remote set-url origin "${REMOTE}";
  fi
  exit 0
  ;;

*)
  echo "unsupported argument $1"
  exit 1
  ;;
esac
