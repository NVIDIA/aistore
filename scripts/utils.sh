#!/bin/bash

function list_all_go_dirs {
  go list -f '{{.Dir}}' "${AISTORE_PATH}/..."
}

function check_gomod {
  if ! go mod tidy -diff &>/dev/null; then
    printf "\nproject requires to run 'go mod tidy'\n"
    exit 1
  fi
}

function check_files_headers {
  for f in $(find ${AISTORE_PATH} -type f -name "*.go" -not -name "*gen.go"  ! -regex $EXTERNAL_SRC_REGEX); do
    # Allow files to start with '//go:build ...', '// Package ...', or '//nolint:...'\
    out=$(head -n 1 $f | grep -P "\/\/(go:build(.*)|\sPackage(.*)|nolint:(.*))")
    if [[ $? -ne 0 ]]; then
      echo "$f: first line should be package a description. Instead got:"
      head -n 1 $f
      exit 1
    fi

    # Expect '// no-copyright' or standard copyright preamble.
    out=$(head -n 10 $f | grep -P "((.*)\/\/\sno-copyright(.*)|(.*)Copyright(.*)NVIDIA(.*)All rights reserved(.*))")
    if [[ $? -ne 0 ]]; then
      echo "$f: copyright statement not found in first 10 lines. Use no-copyright to skip"
      exit 1
    fi
  done
}

function check_deps {
  # Check if `aisloader` package imports `tutils`.
  for f in $(find ${AISTORE_PATH}/bench/tools/aisloader -type f -name "*.go" ! -regex $EXTERNAL_SRC_REGEX); do
    out=$(cat $f | grep '"github.com/NVIDIA/aistore/tutils"')
    if [[ $? -eq 0 ]]; then
      echo "$f: imports 'tutils' package which is forbidden"
      exit 1
    fi
  done
}

function perror {
  err_count=$(echo "$2" | sort -n | uniq | wc -l)
  if [[ -n $2 ]]; then
    echo "${2}" >&2
    echo "$1: ${err_count} failed" >&2
    exit 1
  fi
  exit 0
}
