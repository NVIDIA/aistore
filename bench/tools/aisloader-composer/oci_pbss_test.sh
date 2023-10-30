#!/bin/bash

# dont forget to check the number of aisloader workers and update path to lists below

PBSS_1MB_BUCKET="s3://ais-throughput-test-replica"
PBSS_10MB_BUCKET="s3://ais-ec-10MB"
PBSS_1MB_OBJ_LIST="/PATH/TO/LIST/lists/ais-throughput-test-replica-objects.txt"
PBSS_10MB_OBJ_LIST="/PATH/TO/LIST/lists/ais-ec-10MB-objects.txt"
PBSS_ENDPOINT="https://pbss.s8k.io"

# Verify the accuracy of 'aws_creds' secret. If incorrect, update 'aistore/bench/tools/aisloader-composer/playbooks/cloud/vars/aws_config.yml' with the proper credentials and configuration, then execute the following command.

# source common.sh
# PLAYBOOK=playbooks/cloud/ais_aws_config.yml
# run_ansible_playbook "$PLAYBOOK"

# run 5 minutes of DIRECT on PBSS
AISLOADER_PBSS_ENDPOINT=$PBSS_ENDPOINT AISLOADER_DURATION="5m" AISLOADER_BUCKET=$PBSS_1MB_BUCKET AISLOADER_OBJECTS=$PBSS_1MB_OBJ_LIST ./direct_get_bench.sh
AISLOADER_PBSS_ENDPOINT=$PBSS_ENDPOINT AISLOADER_DURATION="5m" AISLOADER_BUCKET=$PBSS_10MB_BUCKET AISLOADER_OBJECTS=$PBSS_10MB_OBJ_LIST ./direct_get_bench.sh

python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/direct_get/ais-throughput-test-replica/
python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/direct_get/ais-ec-10MB/

# 5 min of COLD GET on AIStore
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$PBSS_1MB_BUCKET AISLOADER_OBJECTS=$PBSS_1MB_OBJ_LIST ./run_get_bench.sh
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$PBSS_10MB_BUCKET AISLOADER_OBJECTS=$PBSS_10MB_OBJ_LIST ./run_get_bench.sh

# option for how long to run
# AISLOADER_EPOCHS=1 OR AISLOADER_DURATION="5m"

python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-throughput-test-replica/
python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-ec-10MB/

cp -r $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-throughput-test-replica $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-throughput-test-replica-old
cp -r $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-ec-10MB $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-ec-10MB-old

# cache all objects
AISLOADER_EPOCHS=1 AISLOADER_BUCKET=$PBSS_1MB_BUCKET AISLOADER_OBJECTS=$PBSS_1MB_OBJ_LIST ./run_get_bench.sh
AISLOADER_EPOCHS=1 AISLOADER_BUCKET=$PBSS_10MB_BUCKET AISLOADER_OBJECTS=$PBSS_10MB_OBJ_LIST ./run_get_bench.sh

# 5 min of WARM GET on AIStore
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$PBSS_1MB_BUCKET AISLOADER_OBJECTS=$PBSS_1MB_OBJ_LIST ./run_get_bench.sh
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$PBSS_10MB_BUCKET AISLOADER_OBJECTS=$PBSS_10MB_OBJ_LIST ./run_get_bench.sh

python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-throughput-test-replica/
python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-ec-10MB/
