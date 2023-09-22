#!/bin/bash

# Run these if desired on initial setup
# ./install_docker.sh
# ./start_grafana.sh
# ./start_netdata.sh

AIS_ENDPOINT=http://10.150.56.227:51080
NUM_DRIVES=30

./reset_buckets.sh $AIS_ENDPOINT bench_1MB
AISLOADER_PUT_SIZE="1MB" AISLOADER_TOTAL_SIZE="10G" BUCKET="bench_1MB" ./run_put_bench.sh
sleep 300
clear_pagecache.sh
AISLOADER_DURATION="1m" BUCKET="bench_1MB" ./run_get_bench.sh
sleep 300
./clear_pagecache.sh

./reset_buckets.sh $AIS_ENDPOINT bench_10MB
AISLOADER_PUT_SIZE="10MB" AISLOADER_TOTAL_SIZE="10G" BUCKET="bench_10MB" ./run_put_bench.sh
sleep 300
./clear_pagecache.sh
AISLOADER_DURATION="1m" BUCKET="bench_10MB" ./run_get_bench.sh
sleep 300
./clear_pagecache.sh

./reset_buckets.sh $AIS_ENDPOINT bench_100MB
AISLOADER_PUT_SIZE="100MB" AISLOADER_TOTAL_SIZE="10G" BUCKET="bench_100MB" ./run_put_bench.sh
sleep 300
./clear_pagecache.sh
AISLOADER_DURATION="1m" BUCKET="bench_100MB" ./run_get_bench.sh

./get_cluster_info.sh $AIS_ENDPOINT
pip3 install -r requirements.txt 
python3 parse_results.py --host_file=inventory.yaml --aisloader_hosts=dgxnodes --total_drives=$NUM_DRIVES
