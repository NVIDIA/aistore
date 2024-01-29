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
## 2. generate large files, run 16 iterations
##    ./ais/test/scripts/ais/test/scripts/s3-mpt-large-files.sh /tmp/largefiles s3://abc 16 true
# #######################################################################################################

# command line
srcdir=$1
bucket=$2

# runtime defaults
iterations=4

prefix="mpt-$RANDOM"

if ! [ -x "$(command -v s3cmd)" ]; then
  echo "Error: s3cmd not installed" >&2
  exit 1
fi

if [ $# -le 1 ]; then
    echo "Usage: $0 DIR BUCKET [NUM-ITERATIONS], where:"
    echo "  DIR:    source directory containing (large) files"
    echo "  BUCKET: destination aistore bucket, e.g., s3://abc (where 's3://abc' may, in fact, be 'ais://abc' etc.)"
    exit 1
fi

if ! [ -d $srcdir ]; then
  echo "Error: directory '$srcdir' does not exist" >&2
  exit 1
fi

if ! [ -z "$3" ]; then
  iterations=$3
fi

## uncomment for verbose output
## set -x

s3cmd info $bucket 1> /dev/null | exit $?

cleanup() {
  rc=$?
  if ! [ -z "$4" ]; then
    rm -f "$srcdir/mpt-f?"
  fi
  s3cmd del $bucket/$prefix/* 2> /dev/null
  exit $rc
}

trap cleanup EXIT INT TERM

if ! [ -z "$4" ]; then
  echo "Generating large files ..."
  dd if=/dev/random of=$srcdir/mpt-f1 bs=4024k count=499 | exit $?
  dd if=/dev/random of=$srcdir/mpt-f2 bs=4024k count=1381 | exit $?
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
    cmd="s3cmd put $srcdir/$f $bucket/$prefix/$f --multipart-chunk-size-mb=$partsize"
    echo "Running '$cmd' ..."
    $cmd || exit $?
  done
    # cleanup
    s3cmd ls $bucket/$prefix/* || exit $?
    echo "Cleaning up ..."
    s3cmd del $bucket/$prefix/* || exit $?
done
