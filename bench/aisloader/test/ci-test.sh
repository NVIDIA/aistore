#!/bin/bash

bucket_name=""
duration="2m"
tar2tf=false

for i in "$@"; do
case $i in
    -b=*|--bucket=*|bucket=*)
        bucket_name="${i#*=}"
        shift
        ;;

    -d=*|--duration=*|duration=*)
        duration="${i#*=}"
        shift
        ;;

    -tar2tf|--tar2tf|tar2tf)
        tar2tf=true
        shift
        ;;
    *)
        usage
        ;;
  esac
done

if [ -z $bucket_name ]; then
  bucket_name=$(cat /dev/urandom | LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
fi

if [ $tar2tf = true ]; then
  # TODO: possibly we could do everything in one call.
  # However, aisloader teminates immediately, if there is no objects in the bucket and pctput != 100.
  aisloader -bucket="$bucket_name" -duration="$duration" -pctput=100 -provider=ais -maxsize=10Mib -minsize=1Mib -totalputsize=5Gib -cleanup=false -readertype=tar -numworkers=8
  aisloader -bucket="$bucket_name" -duration="$duration" -pctput=10 -provider=ais  -maxsize=10Mib -minsize=1Mib -totalputsize=5Gib -cleanup=true -numworkers=8 -transformation=tar2tf
else
  aisloader -bucket="$bucket_name" -duration="$duration" -pctput=100 -provider=ais  -maxsize=10Mib -minsize=1Mib -totalputsize=10Gib -cleanup=false -numworkers=8
  aisloader -bucket="$bucket_name" -duration="$duration" -pctput=10 -provider=ais  -maxsize=10Mib -minsize=1Mib -totalputsize=10Gib -cleanup=true -numworkers=8
fi
