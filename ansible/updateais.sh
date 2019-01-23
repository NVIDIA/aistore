#!/bin/bash
set -o xtrace
set -e
./stopandcleanais.sh 2>&1>/dev/null
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini copyscripts.yml
parallel-ssh -h inventory/cluster.txt -i "./getais.sh "
parallel-ssh -h inventory/cluster.txt -i "./configais.sh "$@
parallel-ssh -h inventory/targets.txt -i "./mountais.sh "$@
if [[ -s inventory/new_targets.txt ]]; then parallel-ssh -h inventory/new_targets.txt -i "./mountais.sh "$@; fi
parallel-ssh -h inventory/targets.txt -i "mount | grep ais"
if [[ -s inventory/new_targets.txt ]]; then parallel-ssh -h inventory/new_targets.txt -i "mount | grep ais"; fi
parallel-ssh -h inventory/cluster.txt -i 'nohup ./enablestats.sh >/dev/null 2>&1' || true
parallel-ssh -h inventory/cluster.txt -i 'ps -leaf | grep statsd' || true
parallel-ssh -h inventory/cluster.txt -i 'service collectd status' || true
./startais.sh

