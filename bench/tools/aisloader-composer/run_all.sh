#!/bin/bash

# sh install_docker.sh
# sh start_grafana.sh
# sh start_netdata.sh

AIS_ENDPOINT=http://10.150.56.227:51080
NUM_DRIVES=30

sh reset_buckets.sh $AIS_ENDPOINT bench_1MB
sh run_playbook_put_1.sh
sleep 300
sh clear_pagecache.sh
sh run_playbook_get_1.sh
sleep 300
sh clear_pagecache.sh

sh reset_buckets.sh $AIS_ENDPOINT bench_10MB
sh run_playbook_put_10.sh
sleep 300
sh clear_pagecache.sh
sh run_playbook_get_10.sh
sleep 300
sh clear_pagecache.sh

sh reset_buckets.sh $AIS_ENDPOINT bench_100MB
sh run_playbook_put_100.sh
sleep 300
sh clear_pagecache.sh
sh run_playbook_get_100.sh

sh get_cluster_info.sh $AIS_ENDPOINT
pip3 install -r requirements.txt 
python3 parse_results.py --host_file=inventory.yaml --aisloader_hosts=dgxnodes --total_drives=$NUM_DRIVES
