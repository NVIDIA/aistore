#!/bin/bash
set -o xtrace
set -e
./stopandcleandfc.sh
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini getdfc.yml
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini configdfc.yml
ansible targets -m shell -a "/home/ubuntu/mountdfc.sh > mountdfc.log" -i inventory/cluster.ini --become
ansible targets -m shell -a "mount | grep dfc" -i inventory/cluster.ini --become
parallel-ssh -h inventory/clients.txt -i "cd /opt/statsd; node ./stats.js ./localConfig.js > /dev/null 2>&1 &"
./startdfc.sh

