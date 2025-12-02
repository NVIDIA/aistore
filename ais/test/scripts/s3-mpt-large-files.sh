#!/bin/bash

# Prerequisites: ########################################################################################
# - aistore cluster
# - s3cmd
# - locally accessible (source) directory that MAY contain large files (see examples below)
# - any aistore bucket
# - xxhsum utility for checksum verification
#
## Examples:
## 1. use existing large files and run 8 iterations
##    s3-mpt-large-files.sh /tmp/largefiles s3://abc 8
#
## 2. generate (the default number of) large files, run 16 iterations
##    s3-mpt-large-files.sh /tmp/largefiles s3://abc 16 true
#
## 3. same as above w/ 10 generated large files
##    s3-mpt-large-files.sh /tmp/largefiles s3://abc 16 true 10
#
## 4. short test mode (single 499MB file)
##    s3-mpt-large-files.sh /tmp/largefiles s3://abc 1 true 1 --short
#
# #######################################################################################################

# command line
srcdir=$1
bucket=$2

# runtime defaults
iterations=4
generate="false"
numfiles=3
short_mode="false"
bucket_created="false"

dstdir="mpt-$RANDOM"
download_dir="/tmp/mpt-downloads-$RANDOM"

# NOTE: to have s3cmd working directly with ais:// bucket
# overriding AWS defaults (incl. "--host=s3.amazonaws.com")
s3endpoint="localhost:8080/s3"
host="--host=$s3endpoint"
host_bucket="--host-bucket=$s3endpoint/%(bucket)"

# SSL configuration based on AIS_USE_HTTPS environment variable
if [[ "${AIS_USE_HTTPS:-}" == "true" ]]; then
  # HTTPS mode: use SSL, skip cert verification for self-signed certs
  ssl_opts="--no-check-certificate"
else
  # HTTP mode: disable SSL
  ssl_opts="--no-ssl"
fi

if ! [ -x "$(command -v s3cmd)" ]; then
  echo "Error: s3cmd not installed" >&2
  exit 1
fi

if ! [ -x "$(command -v xxhsum)" ]; then
  echo "Error: xxhsum not installed" >&2
  exit 1
fi

if ! [ -x "$(command -v ais)" ]; then
  echo "Error: ais (CLI) not installed" >&2
  exit 1
fi

if [ $# -le 1 ]; then
    echo "Usage: $0 DIR BUCKET [NUM-ITERATIONS [GENERATE [NUM-FILES [--short]]]], where:"
    echo "  DIR: source directory containing (large) files"
    echo "  BUCKET: destination aistore bucket, e.g., s3://abc (where 's3://abc' may, in fact, be 'ais://abc' etc.)"
    echo "and optionally:"
    echo "  ITERATIONS: number of iterations to run (default: 4)"
    echo "  GENERATE:   generate large files (default: false)"
    echo "  NUM-FILES:  number of large files to generate (default: 3)"
    echo "  --short:    short test mode (single 499MB file, size validation only)"
    exit 1
fi

## command line parsing
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
if [[ "$6" == "--short" ]]; then
  short_mode="true"
  numfiles=1
  iterations=1
fi

# Auto-detect bucket type and configure endpoints
if [[ "$bucket" == ais://* ]]; then
  bucket_type="ais"
  echo ">> Bucket type: AIS"
elif [[ "$bucket" == s3://* ]]; then
  bucket_type="s3"
  # For real S3, use default endpoints (remove custom host settings)
  host=""
  host_bucket=""
  echo ">> Bucket type: S3"
else
  echo "Error: bucket must start with 's3://' or 'ais://'"
  exit 1
fi

## uncomment for verbose output
## set -x

# Handle bucket existence and setup
if [[ "$bucket_type" == "ais" ]]; then
  # Check if AIS bucket exists, create if not
  if ! ais show bucket "$bucket" -c >/dev/null 2>&1; then
    echo ">> creating AIS bucket: $bucket"
    ais create "$bucket" || exit $?
    bucket_created=true
    # Set checksum type for newly created bucket
    ais bucket props set "$bucket" checksum.type md5 >/dev/null || exit $?
  else
    # Check existing bucket's checksum type
    current_checksum=$(ais bucket props show "$bucket" checksum.type -H | awk '{print $2}')
    if [[ "$current_checksum" != "md5" ]]; then
      echo "Error: existing bucket $bucket has checksum type '$current_checksum', expected 'md5'"
      exit 1
    fi
  fi
  # Verify s3cmd can access the bucket (convert ais:// to s3:// for s3cmd)
  s3_bucket_path="s3://${bucket#ais://}"
  echo ">> verifying s3cmd access to $s3_bucket_path"
  s3cmd info "$s3_bucket_path" $host $host_bucket $ssl_opts 1> /dev/null || exit $?
else
  # Verify S3 bucket exists
  s3cmd info $bucket 1> /dev/null || exit $?
fi

cleanup() {
  rc=$?
  if [[ "$generate" == "true" ]]; then
    rm -f $srcdir/mpt-*
  fi
  # Remove AIS bucket if we created it
  if [[ "$bucket_created" == "true" ]]; then
    echo ">> cleaning up: removing created AIS bucket $bucket"
    ais rmb "$bucket" --yes >/dev/null 2>&1
  fi
  rm -rf "$download_dir" 2> /dev/null

  # Clean up uploaded files
  if [[ "$bucket_type" == "ais" ]]; then
    s3_bucket_path="s3://${bucket#ais://}"
    s3cmd del "$s3_bucket_path/$dstdir/mpt*" $host $host_bucket $ssl_opts 2> /dev/null
  else
    s3cmd del "$bucket/$dstdir/mpt*" 2> /dev/null
  fi
  exit $rc
}

trap cleanup EXIT INT TERM

mkdir -p "$download_dir"

if [[ "$generate" == "true" ]]; then  ## generate
  echo ">> Generating large files (short_mode=$short_mode)..."

  if [[ "$short_mode" == "true" ]]; then
    # Short mode: single 499MB file
    count=499
    echo "   Creating mpt-$count (${count}MB)"
    dd if=/dev/urandom of="$srcdir/mpt-$count" bs=1M count=$count status=progress || exit $?
  else
    # Long mode: multiple files of increasing size
    count=499
    for i in $(seq 1 1 $numfiles); do
      echo "   Creating mpt-$count (${count}MB)"
      dd if=/dev/urandom of="$srcdir/mpt-$count" bs=1M count=$count status=progress || exit $?
      count=$((count + 500))
    done
  fi
fi

files=$(ls $srcdir/mpt-* 2>/dev/null || ls $srcdir)

echo ">> Test mode: $([ "$short_mode" == "true" ] && echo "SHORT" || echo "LONG")"
echo ">> Validation: $([ "$short_mode" == "true" ] && echo "size only" || echo "full checksum")"
echo -e

for i in $(seq 1 1 $iterations); do
  echo "Iteration #$i -------------------------------------------------------------------------"

  # Store file info for validation
  declare -A file_sizes
  declare -A file_checksums

  for f in $files; do
    [[ -f "$srcdir/$f" ]] || continue

    filesize=$(stat --printf="%s" "$srcdir/$f")
    file_sizes["$f"]=$filesize

    if [[ "$short_mode" == "false" ]]; then
      echo "   Computing checksum for $f..."
      checksum=$(xxhsum "$srcdir/$f" | awk '{print $1}')
      file_checksums["$f"]=$checksum
      echo "   $f: size=$filesize, hash=$checksum"
    else
      echo "   $f: size=$filesize"
    fi

    mbs=$(echo "$filesize / 1000 / 1000" | bc)
    parts=$(shuf -i 2-100 -n 1)
    partsize=$(echo "$mbs / $parts" | bc)

    if [ $partsize -le 5 ]; then
      partsize=$(shuf -i 5-10 -n 1)
    fi

    if [[ "$bucket_type" == "ais" ]]; then
      s3_bucket_path="s3://${bucket#ais://}"
      cmd="s3cmd put $srcdir/$f $s3_bucket_path/$dstdir/$f --multipart-chunk-size-mb=$partsize $host $host_bucket $ssl_opts"
    else
      cmd="s3cmd put $srcdir/$f $bucket/$dstdir/$f --multipart-chunk-size-mb=$partsize"
    fi

    echo "   Uploading: $cmd"
    $cmd || exit $?
  done

  # Validation phase
  echo ">> Validating uploaded files..."

  if [[ "$bucket_type" == "ais" ]]; then
    s3_bucket_path="s3://${bucket#ais://}"
    s3cmd ls $s3_bucket_path/$dstdir/* $host $host_bucket $ssl_opts || exit $?
  else
    s3cmd ls $bucket/$dstdir/* || exit $?
  fi

  for f in $files; do
    [[ -f "$srcdir/$f" ]] || continue

    if [[ "$short_mode" == "true" ]]; then
      # Size validation only
      if [[ "$bucket_type" == "ais" ]]; then
        s3_bucket_path="s3://${bucket#ais://}"
        remote_size=$(s3cmd info "$s3_bucket_path/$dstdir/$f" $host $host_bucket $ssl_opts | grep "File size" | awk '{print $3}')
      else
        remote_size=$(s3cmd info "$bucket/$dstdir/$f" | grep "File size" | awk '{print $3}')
      fi

      if [[ "${file_sizes[$f]}" == "$remote_size" ]]; then
        echo "   PASS: $f size validation ($remote_size bytes)"
      else
        echo "   FAIL: $f size mismatch - local=${file_sizes[$f]}, remote=$remote_size"
        exit 1
      fi
    else
      # Full checksum validation
      echo "   Downloading $f for checksum verification..."

      if [[ "$bucket_type" == "ais" ]]; then
        s3_bucket_path="s3://${bucket#ais://}"
        s3cmd get "$s3_bucket_path/$dstdir/$f" "$download_dir/$f" $host $host_bucket $ssl_opts --force || exit $?
      else
        s3cmd get "$bucket/$dstdir/$f" "$download_dir/$f" --force || exit $?
      fi

      downloaded_checksum=$(xxhsum "$download_dir/$f" | awk '{print $1}')

      if [[ "${file_checksums[$f]}" == "$downloaded_checksum" ]]; then
        echo "   PASS: $f checksum validation ($downloaded_checksum)"
      else
        echo "   FAIL: $f checksum mismatch - original=${file_checksums[$f]}, downloaded=$downloaded_checksum"
        exit 1
      fi
    fi
  done

  # cleanup uploaded files
  echo ">> Cleaning up uploaded files..."
  for f in $files; do
    [[ -f "$srcdir/$f" ]] || continue

    if [[ "$bucket_type" == "ais" ]]; then
      s3_bucket_path="s3://${bucket#ais://}"
      s3cmd del $s3_bucket_path/$dstdir/$f $host $host_bucket $ssl_opts
    else
      s3cmd del $bucket/$dstdir/$f
    fi
  done

  # cleanup downloaded files
  rm -f "$download_dir"/*

  echo "Iteration #$i completed successfully"
  echo -e
done

echo ">> All iterations completed successfully!"
echo ">> Test summary: $iterations iteration$([ $iterations -gt 1 ] && echo "s"), $(echo $files | wc -w) file$([ $(echo $files | wc -w) -gt 1 ] && echo "s")"