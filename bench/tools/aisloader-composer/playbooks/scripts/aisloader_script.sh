#!/bin/bash
hostname=$(hostname -a)

outdir=/tmp/aisloader/
sudo rm -rf $outdir
sudo mkdir $outdir

bucket=""
bench_type=""
each_size=""
total_size=""
duration=""
ais_proxies=""
ais_port=""
grafana_host=""
workers=""
filelist=""

for arg in "$@"; do
    case "$arg" in
        --bench_type=*)
            bench_type="${arg#*=}"
            ;;
        --ais_proxies=*)
            ais_proxies="${arg#*=}"
            ;;
        --ais_port=*)
            ais_port="${arg#*=}"
            ;;
        --duration=*)
            duration="${arg#*=}"
            ;;
        --each_size=*)
            each_size="${arg#*=}"
            ;;
        --total_size=*)
            total_size="${arg#*=}"
            ;;
        --grafana_host=*)
            grafana_host="${arg#*=}"
            ;;
        --workers=*)
            workers="${arg#*=}"
            ;;
        --bucket=*)
            bucket="${arg#*=}"
            ;;
        --s3_endpoint=*)
            s3_endpoint="${arg#*=}"
            ;;
        --filelist=*)
            filelist="${arg#*=}"
            ;;
        *)
            echo "Invalid argument: $arg"
            ;;
    esac
done

if [[ "$bench_type" != *"get"* ]] && [[ "$bench_type" != *"put"* ]]; then
  echo "Error: Bench type must contain 'get' or 'put'"
  exit 1
fi

# Parse provider and bucket name from the bucket arg
delimiter="://"
# Check if the input string contains the delimiter
if [[ "$bucket" == *"$delimiter"* ]]; then
    read -r provider bucket_name <<< "$(echo "$bucket" | awk -F "$delimiter" '{print $1 " " $2}')"
else 
    provider="ais"
    bucket_name=$bucket
fi

echo "Running with provider $provider and bucket $bucket_name"

filename="$bucket_name-$bench_type-"
outfile="$outdir$filename$hostname.json"

# Common aisloader args for all bench types
bench_args=("-loaderid=$(hostname)" "-loaderidhashlen=8" "-bucket=$bucket" "-cleanup=false" "-json" "-stats-output=$outfile" "-statsdip=$grafana_host" "-numworkers=$workers")

# Args specific to PUT or GET workloads
if [[ "$bench_type" == *"put"* ]]; then
    bench_args+=("-totalputsize=$total_size")
    bench_args+=("-minsize=$each_size")
    bench_args+=("-maxsize=$each_size")
    bench_args+=("-pctput=100")
    bench_args+=("-skiplist")
else
    bench_args+=("-duration=$duration")
    bench_args+=("-pctput=0")
    if [ -n "$filelist" ]; then
        bench_args+=("-filelist=$filelist")
    fi
fi

# Args specific to either cloud or AIS benchmarks
if [ -n "$s3_endpoint" ]; then
    # Run the benchmark directly to the cloud bucket with the given name and s3endpoint
    bench_args+=("-s3endpoint=$s3_endpoint")
else
    # Run the benchmark against the bucket in AIS
    # Split comma-separated string list of proxies into an array
    IFS=',' read -ra proxy_list <<< "$ais_proxies"

    bench_args+=("-ip=${proxy_list[0]}")
    bench_args+=("-port=$ais_port")
    bench_args+=("-randomproxy") 
fi

# Run the aisloader binary
aisloader "${bench_args[@]}"
