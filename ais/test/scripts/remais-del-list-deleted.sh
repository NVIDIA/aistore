#!/bin/bash

# Randomly delete 10% of the generated files
#
# See related:
# - ais/test/scripts/gen-nested-dirs.sh

# Defaults
root_dir="/tmp/abc"
bucket="ais://@remais/abc"

# usage
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo
    echo "Options:"
    echo "  --root_dir DIR      Root directory containing generated test files"
    echo "  --bucket   BUCKET   Bucket in a remote cluster"
    echo "  -h, --help          Display this help message"
    exit 1
}

## command line
while (( "$#" )); do
  case "${1}" in
    --root_dir) root_dir=$2; shift; shift;;
    --bucket) bucket=$2; shift; shift;;
    -h|--help) usage; exit 0;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done
if [[ "$root_dir" != /tmp/* ]]; then
    echo "Error: expecting root directory to be under /tmp, got: $root_dir"
    exit 1
fi
if [ ! -d "$root_dir" ]; then
    echo "Error: root '$root_dir' does not exist"
    exit 1
fi

## must be @remais
[[ "$bucket" == *//@* ]] || { echo "Error: expecting remote ais bucket, got ${bucket}"; exit 1; }

all_files=( $(find "$root_dir" -type f) )
total_files=${#all_files[@]}

## remais must be attached
rendpoint=$(ais show remote-cluster -H | awk '{print $2}')
[[ ! -z "$rendpoint" ]] || { echo "Error: no remote ais clusters"; exit 1; }
uuid=$(ais show remote-cluster -H | awk '{print $1}')

## actual bucket inside remais:
rbucket="ais://$(basename ${bucket})"

echo "Note: remote ais bucket $bucket is, in fact, ais://@${uuid}/$(basename ${bucket})"
echo

## check remote bucket; create if doesn't exist
exists=true
ais show bucket $bucket -c 1>/dev/null 2>&1 || exists=false
if [[ "$exists" == "false" ]]; then
    ais create $bucket
fi

cleanup() {
  rc=$?
  if [[ "$exists" == "false" ]]; then
     ais rmb $bucket -y 1> /dev/null 2>&1
  fi
  exit $rc
}

trap cleanup EXIT INT TERM

## 2.put generated subtree => remais
AIS_ENDPOINT=$rendpoint ais put $root_dir $rbucket --yes --recursive
ais prefetch $bucket --wait

ais ls $bucket --cached --summary || exit $?
AIS_ENDPOINT=$rendpoint ais ls $rbucket --summary || exit $?

## 3. delete out-of-band
num_files_to_delete=$((total_files / 10))
deleted_files=()
for ((i=0; i<num_files_to_delete; i++)); do
    random_index=$((RANDOM % total_files))
    file_to_delete=${all_files[$random_index]}

    relative_path="${file_to_delete#$root_dir/}"
    deleted_obj="$rbucket/$relative_path"

    AIS_ENDPOINT=$rendpoint ais rmo $deleted_obj 1> /dev/null || exit $?

    deleted_objs+=("$deleted_obj")

    # remove deleted file from array
    all_files=("${all_files[@]:0:$random_index}" "${all_files[@]:$((random_index + 1))}")
    total_files=${#all_files[@]}
done

# Return the deleted pathnames
# echo "Deleted objects:"
for file in "${deleted_objs[@]}"; do
    echo "deleted: $file"
done
