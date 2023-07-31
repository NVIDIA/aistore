#!/bin/bash

# sh install_docker.sh
# sh deploy_grafana.sh
# sh start_netdata.sh

AIS_ENDPOINT=http://10.150.56.227:51080
NUM_DRIVES=30

sh reset_buckets.sh $AIS_ENDPOINT bench_1MB,bench_10MB,bench_100MB

sh run_playbook_put_1.sh
sh run_playbook_get_1.sh
sh run_playbook_put_10.sh
sh run_playbook_get_10.sh
sh run_playbook_put_100.sh
sh run_playbook_get_100.sh

sh get_cluster_info.sh $AIS_ENDPOINT
pip3 install -r requirements.txt 
python3 parse_results.py --host_file=inventory.yaml --aisloader_hosts=dgxnodes --total_drives=$NUM_DRIVES
