#!/bin/bash

set -e

# ./bench.sh cmp OLD_COMMIT [--dir DIRECTORY] [--bench BENCHMARK_NAME]
bench::compare() {
  if [[ $# -lt 1 ]]; then
    echo "fatal: required at least one argument"
  fi

  old_commit=$1
  shift

  # For now only support current commit.
  new_commit="@{-1}"
  current_hash=$(git rev-parse HEAD)

  bench_dir="./..." # By default run in root directory.
  bench_name="."    # By default run all benchmarks.
  while (( "$#" )); do
    case "${1}" in
      --dir) bench_dir="./$2/..."; shift; shift;;
      --bench) bench_name=$2; shift; shift;;
      *) echo "fatal: unknown argument '${1}'"; exit 1;;
    esac
  done

  old_results=$(mktemp)
  new_results=$(mktemp)
  echo "Running benches on: ${old_commit}"
  git checkout -q ${old_commit}
  go test -run=NONE -bench=${bench_name} ${bench_dir} > ${old_results}
  echo "Running benches on: ${current_hash}"
  git checkout -q ${new_commit}
  go test -run=NONE -bench=${bench_name} ${bench_dir} > ${new_results}
  benchcmp ${old_results} ${new_results}
}

case "${1}" in
  cmp) shift; bench::compare $@ ;;
  *) echo "fatal: unknown argument '${1}'"; exit 1;;
esac
