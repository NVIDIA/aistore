#!/bin/bash
set -o xtrace
set -e
./stopandcleandfc.sh 2>&1>/dev/null
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini copyscripts.yml
parallel-ssh -h inventory/cluster.txt -i "./getdfc.sh "$@
parallel-ssh -h inventory/cluster.txt -i "./configdfc.sh "$@
parallel-ssh -h inventory/targets.txt -i "./mountdfc.sh "$@
parallel-ssh -h inventory/new_targets.txt -i "./mountdfc.sh "$@
parallel-ssh -h inventory/targets.txt -i "mount | grep dfc"
parallel-ssh -h inventory/new_targets.txt -i "mount | grep dfc"
parallel-ssh -h inventory/cluster.txt -i 'nohup ./enablestats.sh >/dev/null 2>&1' || true
parallel-ssh -h inventory/cluster.txt -i 'ps -leaf | grep statsd' || true
parallel-ssh -h inventory/cluster.txt -i 'service collectd status' || true
./startdfc.sh

