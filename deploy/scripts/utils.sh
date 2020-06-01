#!/bin/bash
function list_all_go_dirs {
  go list -f '{{.Dir}}' "${AISTORE_DIR}/..."
}

function check_files_headers {
  for f in $(find ${AISTORE_DIR} -type f -name "*.go" ! -regex $EXTERNAL_SRC_REGEX); do
    out=$(head -n 1 $f | grep -P "(\s*+build.*|\/(\/|\*)(\s)*Package(.)*)$")
    if [[ $? -ne 0 ]]; then
      echo "$f: first line should be package a description. Instead got:"
      head -n 1 $f
      exit 1
    fi

    out=$(head -n 10 $f | grep -P "((.)*no-copyright(.)*|(.)*NVIDIA(.)*All rights reserved(.)*)")
    if [[ $? -ne 0 ]]; then
      echo "$f: copyright statement not found in first 10 lines. Use no-copyright to skip"
      exit 1
    fi
  done
}

function check_imports {
  # Check if `import` block contains more than one empty line.
  for f in $(find ${AISTORE_DIR} -type f -name "*.go" ! -regex $EXTERNAL_SRC_REGEX); do
    # https://regexr.com/55u6r
    out=$(head -n 50 $f | grep -Pz 'import \((.|\n)*(\n\n)+(\t(\w|\.)?\s?(.*)"(.*)"\n)*\n+(\t(\w|\.)?\s?"(.*)"\n)*\)')
    if [[ $? -eq 0 ]]; then
      echo "$f: 'import' block contains (at least) 2 empty lines (expected: 1)"
      exit 1
    fi
  done
}

function check_python_formatting {
  i=0
  for f in $(find . -type f -name "*.py" ! -regex ".*__init__.py" ! -regex $EXTERNAL_SRC_REGEX); do
    pylint --rcfile=$PYLINT_STYLE $f --msg-template="{path} ({C}):{line:3d},{column:2d}: {msg} ({msg_id}:{symbol})" 2>/dev/null
    if [[ $? -gt 0 ]]; then i=$((i+1)); fi
  done

  if [[ $i -ne 0 ]]; then
    printf "\npylint failed, fix before continuing\n"
    exit 1
  fi

  i=0
  for f in $(find . -type f -name "*.py" ! -regex $EXTERNAL_SRC_REGEX); do
     yapf -d --style=$YAPF_STYLE $f
     if [[ $? -gt 0 ]]; then i=$((i+1)); fi
  done

  if [[ $i -ne 0 ]]; then
    printf "\nIncorrect python formatting. Run make fmt-fix to fix it.\n\n" >&2
    exit 1
  fi
}

function python_yapf_fix {
  find . -type f -name "*.py" ! -regex $EXTERNAL_SRC_REGEX -exec yapf -i --style=$YAPF_STYLE {} ";"
}

function perror {
  err_count=$(echo "$2" | sort -n | uniq | wc -l)
  if [[ -n ${err_count} ]]; then
    echo "${err_count}" >&2
    echo "$1: ${err_count} failed" >&2
    exit 1
  fi
  exit 0
}
