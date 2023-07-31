#!/bin/bash

bucket_name=""
duration="2m"
etl=false

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

    -etl|--etl)
        etl=true
        shift
        ;;
    *)
        usage
        ;;
  esac
done

if [[ -z ${bucket_name} ]]; then
  bucket_name=$(cat /dev/urandom | LC_CTYPE=C tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1)
fi

if [ "$etl" = true ]; then
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=100 -provider=ais -maxsize=10Mib -minsize=1Mib -totalputsize=5Gib -cleanup=false -numworkers=8 -readertype=tar
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=10 -provider=ais  -maxsize=10Mib -minsize=1Mib -totalputsize=5Gib -cleanup=true  -numworkers=8 -readertype=tar -etl=tar2tf
else
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=100 -provider=ais -maxsize=10Mib -minsize=1Mib -totalputsize=10Gib -cleanup=false -numworkers=8
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=10 -provider=ais  -maxsize=10Mib -minsize=1Mib -totalputsize=10Gib -cleanup=true  -numworkers=8
fi
