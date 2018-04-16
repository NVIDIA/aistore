#!/bin/bash
set -o xtrace
set -e
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini getdfc.yml
ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini configdfc.yml
ansible targets -m shell -a "/home/ubuntu/mountdfc.sh > mountdfc.log" -i inventory/cluster.ini --become
ansible targets -m shell -a "mount | grep dfc" -i inventory/cluster.ini --become
echo Starting primary proxy
parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/startprimaryproxy.sh >/dev/null 2>&1'
echo Starting stub proxies on every target node
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/startproxy.sh >/dev/null 2>&1'
echo Starting all targets
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/starttarget.sh >/dev/null 2>&1'
echo Wait 10 seconds before checking DFC health
sleep 10
parallel-ssh -h inventory/proxy.txt -i 'ps -C dfc'
parallel-ssh -h inventory/targets.txt -i 'ps -C dfc'
parallel-ssh -h inventory/proxy.txt -i 'cat ~/dfc.json'
parallel-ssh -h inventory/proxy.txt -i 'tail -20 /var/log/dfc/dfc.INFO'
