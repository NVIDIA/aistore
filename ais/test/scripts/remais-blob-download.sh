#!/bin/bash

## Prerequisites: ###############################################################################
# - aistore cluster
# - remote aistore cluster (a.k.a. "remais")
# - optionally, remais bucket (the bucket will be created if doesn't exist)
# - ais (CLI)
# - aisloader

## Example usage: first, make sure remote ais cluster is attached, e.g.:
#  $ ais show remote-cluster -H
#  $ JcHy3JUrL  http://127.0.0.1:11080  remais    v9  1  11m22.312048996s
#
## Examples:
#  $./remais-blob-download.sh --bucket ais://@remais/abc --maxsize 10mb --totalsize 1G
#
#  $./remais-blob-download.sh --bucket ais://@remais/abc --numworkers 5 --chunksize 500kb
#################################################################################################

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi
if ! [ -x "$(command -v aisloader)" ]; then
  echo "Error: aisloader (benchmark tool) not installed" >&2
  exit 1
fi

## Command line options and respective defaults
bucket="ais://@remais/abc"
minsize="10MiB"
maxsize="10MiB"
totalsize="100MiB"
chunksize="1MB"
numworkers=4

## runtime
max_num_downloads=100
subdir="blob-$RANDOM" ## destination for aisloader-generated content

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    --minsize) minsize=$2; shift; shift;;
    --maxsize) maxsize=$2; shift; shift;;
    --totalsize) totalsize=$2; shift; shift;;
    --chunksize) chunksize=$2; shift; shift;;
    --numworkers) numworkers=$2; shift; shift;;
    *) echo "fatal: unknown argument '${1}'"; exit 1;;
  esac
done

## uncomment for verbose output
## set -x

## must be @remais
[[ "$bucket" == *//@* ]] || { echo "Error: expecting remote ais bucket, got ${bucket}"; exit 1; }

## actual bucket inside remais:
rbucket="ais://$(basename ${bucket})"

## remais must be attached
rendpoint=$(ais show remote-cluster -H | awk '{print $2}')
[[ ! -z "$rendpoint" ]] || { echo "Error: no remote ais clusters"; exit 1; }
uuid=$(ais show remote-cluster -H | awk '{print $1}')

echo "Note: remote ais bucket $bucket is, in fact, ais://@${uuid}/$(basename ${bucket})"

## check remote bucket; create if doesn't exist
exists=true
ais show bucket $bucket -c 1>/dev/null 2>&1 || exists=false

cleanup() {
  rc=$?
  if [[ "$exists" == "true" ]]; then
    AIS_ENDPOINT=$rendpoint ais rmo $rbucket --prefix=$subdir/ 2> /dev/null
    ais rmo $bucket --prefix=$subdir/ 2> /dev/null
  else
    ais rmb $bucket -y 1> /dev/null 2>&1
  fi
  exit $rc
}

trap cleanup EXIT INT TERM

## aisloader => remais, to generate (PUT) content in $bucket/$subdir
echo "Running aisloader now..."
AIS_ENDPOINT=$rendpoint aisloader -bucket=$rbucket -subdir=$subdir -cleanup=false -numworkers=2 -quiet -pctput=100 -minsize=$minsize -maxsize=$maxsize -totalputsize=$totalsize

ais ls $bucket --all --limit 4
echo "..."
files=$(ais ls $bucket --prefix=$subdir/ --name-only -H --no-footers --all | awk '{print $1}')

## put some limit to it
count=0

## for all listed objects
for f in $files; do
  xid=$(ais blob-download $bucket/$f --chunk-size $chunksize --num-workers $numworkers --nv || exit $?)
  ais wait $xid || exit $?
  count=`expr $count + 1`
  if [ $count -ge $max_num_downloads ]; then
     break
  fi
done

echo "..."
ais show job blob-download --all | tail
