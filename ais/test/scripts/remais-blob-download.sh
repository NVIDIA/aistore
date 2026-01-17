#!/bin/bash

## Prerequisites: ###############################################################################
# - aistore cluster
# - remote aistore cluster (a.k.a. "remais")
# - optionally, remais bucket (the bucket will be created if doesn't exist)
# - ais (CLI)
# - aisloader
#
# NOTE: max_num_downloads limit (hardcoded)
#
## Example 1:
## first, make sure remote ais cluster is attached:
#  $ ais show remote-cluster -H
#  $ JcHy3JUrL  http://127.0.0.1:11080  remais    v9  1  11m22.312048996s
#
## second, run:
#  $ remais-blob-download.sh --bucket ais://@remais/abc --maxsize 10mb --totalsize 1G
#
## Example 2:
#  $ remais-blob-download.sh --bucket ais://@remais/abc --chunksize 500kb
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

## aisloader command line
minsize="10MiB"
maxsize="1GiB"
totalsize="10GiB"

## permutating (1mb, 2mb, 3mb) unless explicitly given in the command line
chunksize="1MB"

## put some limit to it
max_num_downloads=30 ########## ---------------------------------------------------------------------

## destination for aisloader-generated content
subdir="blob-$RANDOM"

## initial number of the chunk-reading workers
numworkers_initial=3

while (( "$#" )); do
  case "${1}" in
    --bucket) bucket=$2; shift; shift;;
    --minsize) minsize=$2; shift; shift;;
    --maxsize) maxsize=$2; shift; shift;;
    --totalsize) totalsize=$2; shift; shift;;
    --chunksize) chunksize=$2; shift; shift;;
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
echo

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

chunk_size() {
  if [ $chunksize == "1MB" ]; then
    chunksize="2MB"
  elif [ $chunksize == "2MB" ]; then
    chunksize="3MB"
  else
    chunksize="1MB"
  fi
}

## aisloader => remais, to generate (PUT) content in $bucket/$subdir
echo "1. Run aisloader"
AIS_ENDPOINT=$rendpoint aisloader -bucket=$rbucket -subdir=$subdir -cleanup=false -numworkers=2 -quiet -pctput=100 -minsize=$minsize -maxsize=$maxsize -totalputsize=$totalsize

ais ls $bucket --all --limit 4
echo "..."
files=$(ais ls $bucket --prefix=$subdir/ --name-only -H --no-footers --all | awk '{print $1}')

count=0 ## up to max_num_downloads

## first, run as xaction
echo
echo "2. Run blob-download jobs"
numworkers=$numworkers_initial
for f in $files; do
  xid=$(ais blob-download $bucket/$f --chunk-size $chunksize --num-workers $numworkers --nv || exit $?)
  ais wait $xid >/dev/null || exit $?
  count=`expr $count + 1`
  if [ $count -ge $max_num_downloads ]; then
     break
  fi
  numworkers=`expr $numworkers + 1`
  if [ $numworkers -ge 16 ]; then
     numworkers=$numworkers_initial
  fi
  chunk_size
done

echo "2.a. All downloads must finish -----------------------------------------"
ais show job blob-download

## show some tails
echo "2.b. Show some tails -----------------------------------------"
echo "..."
ais show job blob-download --all | tail
echo "..."
ais ls $bucket --cached | tail

## second, run the same via GET
echo
echo "3. Run GET via blob-downloader - evict first..."
ais evict $bucket --keep-md || exit $?
count=0
numworkers=$numworkers_initial
for f in $files; do
  ais get $bucket/$f /dev/null --blob-download --num-workers=$numworkers >/dev/null || exit $?
  count=`expr $count + 1`
  if [ $count -ge $max_num_downloads ]; then
     break
  fi
  numworkers=`expr $numworkers + 1`
  if [ $numworkers -ge 16 ]; then
     numworkers=$numworkers_initial
  fi
  chunk_size
done

## uncomment if need be
## ais show job blob-download | grep Running

echo "3.a. Wait for all blob-downloads (do not run concurrently with other blob-download tests!)"
ais wait blob-download || exit $?

echo "3.b. All downloads must finish -----------------------------------------"
ais show job blob-download

## ditto
echo "3.c. Show some tails -----------------------------------------"
echo "..."
ais show job blob-download --all | tail
echo "..."
ais ls $bucket --cached | tail
