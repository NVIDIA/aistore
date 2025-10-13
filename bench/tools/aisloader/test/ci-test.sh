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
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=100 -provider=ais -maxsize=10MiB -minsize=1MiB -totalputsize=5GiB -cleanup=false -numworkers=8 -readertype=tar --quiet
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=10 -provider=ais  -maxsize=10MiB -minsize=1MiB -totalputsize=5GiB -cleanup=false -numworkers=8 -readertype=tar -etl=tar2tf --quiet

  # GET(batch) smoke test
  aisloader -bucket="${bucket_name}" -duration=30s -pctput=0 -provider=ais -cleanup=true -numworkers=4 -get-batchsize=32 --quiet
else
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=100 -provider=ais -maxsize=10MiB -minsize=1MiB -totalputsize=10GiB -cleanup=false -numworkers=8 --quiet
  aisloader -bucket="${bucket_name}" -duration="$duration" -pctput=10 -provider=ais  -maxsize=10MiB -minsize=1MiB -totalputsize=10GiB -cleanup=false -numworkers=8 --quiet

  # GET(batch) smoke test
  aisloader -bucket="${bucket_name}" -duration=45s -pctput=0 -provider=ais -cleanup=true -numworkers=4 -get-batchsize=64 --quiet
fi
