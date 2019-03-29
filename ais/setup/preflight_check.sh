#!/bin/bash

# This script is used by Makefile to run preflight-checks.

case $CHECK in
lint)
    echo "Running lint check..." >&2
    err_count=0
    echo "Checking vet..." >&2
    for dir in ${LINT_DIRS}
    do
        errs=$(go vet "${LINT_VET_IGNORE}" "${dir}" 2>&1 | tee -a /dev/stderr | wc -l)
        err_count=$(($err_count + $errs))
    done
    echo "Checking staticcheck..." >&2
    errs=$(${GOPATH}/bin/gometalinter --deadline 5m --exclude="${LINT_IGNORE}" --disable-all --enable=staticcheck --linter="staticcheck:staticcheck -tags hrw {path}:PATH:LINE:COL:MESSAGE" ${LINT_DIRS} 2>&1 | tee -a /dev/stderr | wc -l )
    err_count=$(($err_count + $errs))
    echo "Checking others..." >&2
    errs=$(${GOPATH}/bin/gometalinter --deadline 5m --exclude="${LINT_IGNORE}" --disable-all --enable=golint --enable=errcheck ${LINT_DIRS} 2>&1 | tee -a /dev/stderr | wc -l )
    err_count=$(($err_count + $errs))
    if [ "${err_count}" != "0" ]; then
        echo "found ${err_count} lint errors, please fix or add exception" >&2
        exit 1
    fi
    exit 0
  ;;
fmt)
    err_count=0
    echo "Running style check..." >&2
    for dir in ${LINT_DIRS}
    do
        errs=$(${GOPATH}/bin/goimports -d ${dir} 2>&1 | tee -a /dev/stderr | grep -e "^diff -u" | wc -l)
        err_count=$(($err_count + $errs))
    done
    if [ "${err_count}" != "0" ]; then
        echo "found ${err_count} style errors, run 'make fmt-fix' to fix" >&2
        exit 1
    fi
    exit 0
  ;;
spell)
    err_count=0
    echo "Running spell check..." >&2
    for dir in ${MISSPELL_DIRS}
    do
        errs=$(${GOPATH}/bin/misspell "${dir}" 2>&1 | tee -a /dev/stderr | wc -l)
        err_count=$(($err_count + $errs))
    done
    if [ "${err_count}" != "0" ]; then
        echo "found ${err_count} spelling errors, check to confirm, then run 'make spell-fix' to fix" >&2
        exit 1
    fi
    exit 0
  ;;
test-env)
hash docker &>/dev/null
if [ "$?" == "0" ]; then
    docker_running=$(docker container ls)
    if [ "$?" != "0" ]; then
        echo "Warning: Can't check if AIS is running from docker, verify that you have permissions for /var/run/docker.sock" >&2
    elif [ "$(echo ${docker_running} | grep ais)" != "" ]; then
        echo "AIStore running on docker..." >&2
        exit 0 
    fi
fi
if [ "$(echo $KUBERNETES_SERVICE_HOST)" != "" ]; then 
  echo "AIStore running on Kubernetes..." >&2
  if [ "${AISURL}" != "" ]; then
      ip=(${AISURL//:/ }[0])
      if [ "$(ping -c 1 ${ip} | grep '1 received')" != "" ]; then
          echo "AIStore connection to ${ip} is working..." >&2
          exit 0
      else 
          echo "Error connecting to ${ip}. Did you specify the correct address?" >&2
          exit 1
      fi
  else
      echo "Error missing environment variable: 'AISURL=<IP>:<PORT>'" >&2
      exit 1
  fi
fi
if [ "$(ps aux | grep -v -e 'grep' | grep bin/aisnode)" != "" ]; then
    echo "AIStore running locally..." >&2
    exit 0 
fi

echo "AIStore is not running, this causes some tests to fail! (to run, see: https://github.com/NVIDIA/aistore#local-non-containerized)" >&2
echo -n "continue? [y/N] " >&2 && read ans && [ ${ans:-N} == y ]
exit $?
  ;;
test-short)
  echo "Running short tests..." >&2
  errs=$(BUCKET=${BUCKET} AISURL=${AISURL} go test -v -p 1 -count 1 -short ../... 2>&1 | tee -a /dev/stderr | grep -e "^FAIL\|^--- FAIL" )
  err_count=$(echo "${errs}" | wc -l)
  if [ ! -z "${errs}" ]; then
      echo "${errs}" >&2
      echo "test-short: ${err_count} failed" >&2
      exit 1
  fi
  exit 0
  ;;
test-long)
  echo "Running long tests..." >&2
  errs=$(BUCKET=${BUCKET} AISURL=${AISURL} go test -v -p 1 -count 1 -timeout 2h ../... 2>&1 | tee -a /dev/stderr | grep -e "^FAIL\|^--- FAIL" )
  err_count=$(echo "${errs}" | wc -l)
  if [ ! -z "${errs}" ]; then
      echo "${errs}" >&2
      echo "test-short: ${err_count} failed" >&2
      exit 1
  fi
  exit 0
  ;;
test-run)
  echo "Running test with regex..." >&2
  errs=$(BUCKET=${BUCKET} AISURL=${AISURL} go test -v -p 1 -count 1 -timeout 2h  -run="${RE}" ../... 2>&1 | tee -a /dev/stderr | grep -e "^FAIL\|^--- FAIL" )
  err_count=$(echo "${errs}" | wc -l)
  if [ ! -z "${errs}" ]; then
      echo "${errs}" >&2
      echo "test-run: ${err_count} failed" >&2
      exit 1
  fi
  exit 0
  ;;
"")
  echo "missing environment variable: CHECK=\"checkname\""
  exit 1
  ;;
*)
  echo "unsupported check: $CHECK"
  exit 1
  ;;
esac

echo $Message | mail -s "disk report `date`" anny
