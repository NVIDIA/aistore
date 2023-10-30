#!/bin/bash

# dont forget to check the number of aisloader workers and update path to lists below

S3_1MB_BUCKET="s3://ais-vm"
S3_10MB_BUCKET="s3://ais-jm"
S3_1MB_OBJ_LIST="/Users/abhgaikwad/lists/ais-vm-objects.txt"
S3_10MB_OBJ_LIST="/Users/abhgaikwad/lists/ais-jm-objects.txt"
S3_ENDPOINT="https://s3.amazonaws.com"

# Verify the accuracy of 'aws_creds' secret. If incorrect, update 'aistore/bench/tools/aisloader-composer/playbooks/cloud/vars/aws_config.yml' with the proper credentials and configuration, then execute the following commands.

# source common.sh
# PLAYBOOK=playbooks/cloud/ais_aws_config.yml
# run_ansible_playbook "$PLAYBOOK"

# run 5 minutes of DIRECT on S3
AISLOADER_S3_ENDPOINT=$S3_ENDPOINT AISLOADER_DURATION="5m" AISLOADER_BUCKET=$S3_1MB_BUCKET AISLOADER_OBJECTS=$S3_1MB_OBJ_LIST ./direct_get_bench.sh
AISLOADER_S3_ENDPOINT=$S3_ENDPOINT AISLOADER_DURATION="5m" AISLOADER_BUCKET=$S3_10MB_BUCKET AISLOADER_OBJECTS=$S3_10MB_OBJ_LIST ./direct_get_bench.sh

python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/direct_get/ais-vm/
python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/direct_get/ais-jm/

# 5 min of COLD GET on AIStore
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$S3_1MB_BUCKET AISLOADER_OBJECTS=$S3_1MB_OBJ_LIST ./run_get_bench.sh
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$S3_10MB_BUCKET AISLOADER_OBJECTS=$S3_10MB_OBJ_LIST ./run_get_bench.sh

# option for how long to run
# AISLOADER_EPOCHS=1 OR AISLOADER_DURATION="5m"

python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-vm/
python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-jm/

cp -r $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-vm $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-vm-old
cp -r $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-jm $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-jm-old

# cache all objects
AISLOADER_EPOCHS=1 AISLOADER_BUCKET=$S3_1MB_BUCKET AISLOADER_OBJECTS=$S3_1MB_OBJ_LIST ./run_get_bench.sh
AISLOADER_EPOCHS=1 AISLOADER_BUCKET=$S3_10MB_BUCKET AISLOADER_OBJECTS=$S3_10MB_OBJ_LIST ./run_get_bench.sh

# 5 min of WARM GET on AIStore
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$S3_1MB_BUCKET AISLOADER_OBJECTS=$S3_1MB_OBJ_LIST ./run_get_bench.sh
./clear_pagecache.sh
AISLOADER_DURATION="5m" AISLOADER_BUCKET=$S3_10MB_BUCKET AISLOADER_OBJECTS=$S3_10MB_OBJ_LIST ./run_get_bench.sh

python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-vm/
python consolidate_results.py $GOPATH/src/github.com/NVIDIA/aistore/bench/tools/aisloader-composer/output/get/ais-jm/
