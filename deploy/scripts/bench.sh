#!/bin/bash

set -e

run_test() {
  commit=$1
  results=$2
  echo "Running benches on: ${commit}"

  git checkout -q $commit
  if [[ -n ${post_checkout} ]]; then
    eval "${post_checkout}"
  fi

  test="BUCKET=${BUCKET} AIS_ENDPOINT=${AIS_ENDPOINT} go test ${verbose} -p 1 -parallel 4 -count ${count} -timeout 2h -run=NONE -bench=${bench_name} ${bench_dir}"
  if [[ -n ${verbose} ]]; then
    eval $test | tee -a $results
  else
    eval $test > $results
  fi
}

# ./bench.sh cmp [--old-commit OLD_COMMIT] [--dir DIRECTORY] [--bench BENCHMARK_NAME] [--post_checkout SCRIPT_NAME] [--verbose]
bench::compare() {
  old_commit=HEAD^

  # For now only support current commit.
  new_commit="@{-1}"
  current_hash=$(git rev-parse HEAD)

  bench_dir="./..." # By default run in root directory.
  bench_name="."    # By default run all benchmarks.
  verbose=""
  count=1
  while (( "$#" )); do
    case "${1}" in
      --dir) bench_dir="$2/..."; shift; shift;;
      --bench) bench_name=$2; shift; shift;;
      --post-checkout) post_checkout=$2; shift; shift;;
      --verbose) verbose="-v"; shift;;
      --count) count=$2; shift; shift;;
      --old-commit) old_commit=$2; shift; shift;;
      *) echo "fatal: unknown argument '${1}'"; exit 1;;
    esac
  done
  old_results=$(mktemp)
  new_results=$(mktemp)
  run_test ${old_commit} ${old_results}
  run_test ${current_hash} ${new_results}
  benchcmp ${old_results} ${new_results}
  errs=$(grep -ae "^--- FAIL: Bench" ${old_results} ${new_results})
  perror $1 "${errs}"
}

case "${1}" in
  cmp) shift; bench::compare "${@}" ;;
  *) echo "fatal: unknown argument '${1}'"; exit 1;;
esac
