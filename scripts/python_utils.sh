#!/bin/bash

PYLINT_STYLE="${AISTORE_PATH}/scripts/config/.pylintrc"
PYTHON_SDK_DIR="${AISTORE_PATH}/python"

function lint_python_outside_sdk {
    local fail_count=0
    files=$(find . -type f -name "*.py" ! -name "__init__.py" ! -regex "$EXTERNAL_SRC_REGEX" \
            ! -path "*/venv/*" \
            ! -path "*/docs/examples/*" \
            ! -path "*/python/*"
          )
    # Check any python code not in aistore/python
    for f in $files; do
      pylint --rcfile="$PYLINT_STYLE" "$f" --msg-template="{path} ({C}):{line:3d},{column:2d}: {msg} ({msg_id}:{symbol})" 2>/dev/null
      if [[ $? -gt 0 ]]; then fail_count=$((fail_count+1)); fi
    done
    printf "Found %d python lint failures outside Python SDK\n" $fail_count
    return $fail_count
}

# Check the python code in aistore/python using the make targets there
function lint_python_sdk {
    cd "$PYTHON_SDK_DIR" || return 1
    echo "in $PYTHON_SDK_DIR"
    make lint || return 1
    echo "passed lint"
    make lint-tests || return 1
    echo "passed lint tests"
    return 0
}

function python_black_fix {
  black . --quiet --force-exclude examples
}

function check_python_formatting {
  if ! black . --check --diff --quiet --extend-exclude examples
  then
    printf "\nIncorrect python formatting. Run make fmt-fix to fix it.\n\n" >&2
    exit 1
  fi
}