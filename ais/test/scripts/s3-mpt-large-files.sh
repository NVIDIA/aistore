#!/bin/bash

# Prerequisites: ########################################################################################
# - aistore cluster
# - s3cmd
# - locally accessible directory that MAY contain large files (see examples below)
# - any aistore bucket
#
## Example usage:
## 1. use existing large files and run 8 iterations
##    ./ais/test/scripts/ais/test/scripts/s3-mpt-large-files.sh /tmp/largefiles s3://abc 8
## 2. generate (the default number of) large files, run 16 iterations
##    ./ais/test/scripts/ais/test/scripts/s3-mpt-large-files.sh /tmp/largefiles s3://abc 16 true
## 3. same as above w/ 10 generated large files
##    ./ais/test/scripts/ais/test/scripts/s3-mpt-large-files.sh /tmp/largefiles s3://abc 16 true 10
# #######################################################################################################

# command line
srcdir=$1
bucket=$2

# runtime defaults
iterations=4
generate="false"
numfiles=3

prefix="mpt-$RANDOM"

# NOTE: to have s3cmd working directly with ais:// bucket
# overriding AWS defaults (incl. "--host=s3.amazonaws.com")
s3endpoint="localhost:8080/s3"
host="--host=$s3endpoint"
host_bucket="--host-bucket=$s3endpoint/%(bucket)"

if ! [ -x "$(command -v s3cmd)" ]; then
  echo "Error: s3cmd not installed" >&2
  exit 1
fi

if [ $# -le 1 ]; then
    echo "Usage: $0 DIR BUCKET [NUM-ITERATIONS [GENERATE [NUM-FILES]]], where:"
    echo "  DIR: source directory containing (large) files"
    echo "  BUCKET: destination aistore bucket, e.g., s3://abc (where 's3://abc' may, in fact, be 'ais://abc' etc.)"
    echo "and optionally:"
    echo "  ITERATIONS: number of iterations to run (default: 4)"
    echo "  GENERATE:   generate large files (default: false)"
    echo "  NUM-FILES:  number of large files to generate (default: 3)"
    exit 1
fi

## command line
if ! [ -d $srcdir ]; then
  echo "Error: directory '$srcdir' does not exist" >&2
  exit 1
fi
if ! [ -z "$3" ]; then
  iterations=$3
fi
if ! [ -z "$4" ]; then
  generate=$4
fi
if ! [ -z "$5" ]; then
  numfiles=$5
fi

## uncomment for verbose output
## set -x

s3cmd info $bucket  $host $host_bucket --no-ssl 1> /dev/null | exit $?

cleanup() {
  rc=$?
  if [[ "$generate" == "true" ]]; then
    rm -f $srcdir/mpt-*
  fi
  s3cmd del "$bucket/$prefix/mpt*"  $host $host_bucket --no-ssl 2> /dev/null
  exit $rc
}

trap cleanup EXIT INT TERM

if [[ "$generate" == "true" ]]; then  ## generate
  echo "Generating large files ..."
  count=499
  for i in $(seq 1 1 $numfiles); do
    dd if=/dev/random of="$srcdir/mpt-$count" bs=4024k count=$count | exit $?
    count=`expr $count + 500`
  done
fi

files=`ls $srcdir`
for i in {1..$iterations}; do
  echo "Iteration #$i -------------------------------------------------------------------------"
  for f in $files; do
    filesize=$(stat --printf="%s" $srcdir/$f)
    mbs=$(echo "$filesize / 1000 / 1000" | bc)
    parts=$(shuf -i 2-100 -n 1)
    partsize=$(echo "$mbs / $parts" | bc)

    if [ $partsize -le 5 ]; then
      partsize=$(shuf -i 5-10 -n 1)
    fi
    cmd="s3cmd put $srcdir/$f $bucket/$prefix/$f --multipart-chunk-size-mb=$partsize $host $host_bucket --no-ssl"
    echo "Running '$cmd' ..."
    $cmd || exit $?
  done
    # cleanup
    s3cmd ls $bucket/$prefix/* $host $host_bucket --no-ssl || exit $?
    echo "Cleaning up ..."
    s3cmd del $bucket/$prefix/* $host $host_bucket --no-ssl || exit $?
done
