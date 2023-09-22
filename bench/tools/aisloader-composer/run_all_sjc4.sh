#!/bin/bash

# Run these if desired on initial setup
# sh install_docker.sh
# sh start_grafana.sh
# sh start_netdata.sh

REPLICA_OBJECT_LIST="/Users/aawilson/ais-throughput-test-replica-objects.txt"
EC_OBJECT_LIST="/Users/aawilson/ais-ec-10MB.txt"

./clear_pagecache.sh
# Direct benchmark from PBSS 1MB bucket
AISLOADER_DURATION="2m" AISLOADER_BUCKET="s3://ais-throughput-test-replica" AISLOADER_OBJECTS=$REPLICA_OBJECT_LIST ./direct_get_bench.sh
sleep 30
./clear_pagecache.sh

# Cached benchmark through AIS 1MB bucket
AISLOADER_DURATION="2m" AISLOADER_BUCKET="s3://ais-throughput-test-replica" AISLOADER_OBJECTS=$REPLICA_OBJECT_LIST ./run_get_bench.sh
sleep 30
./clear_pagecache.sh

# Direct benchmark from pbss 10MB bucket
AISLOADER_DURATION="2m" AISLOADER_BUCKET="s3://ais-ec-10MB" AISLOADER_OBJECTS=$EC_OBJECT_LIST ./direct_get_bench.sh
sleep 30
./clear_pagecache.sh

# Cached benchmark through AIS 10MB bucket
AISLOADER_DURATION="2m" AISLOADER_BUCKET="s3://ais-ec-10MB" AISLOADER_OBJECTS=$EC_OBJECT_LIST ./run_get_bench.sh
sleep 30
./clear_pagecache.sh