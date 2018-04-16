#!/bin/bash
set -o xtrace
set -e
#ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini getdfc.yml
#ANSIBLE_HOST_KEY_CHECKING=False ansible-playbook -i inventory/cluster.ini configdfc.yml
ansible targets -m copy -a "src=mountdfc.sh dest=/home/ubuntu/mountdfc.sh" -i inventory/cluster.ini --become
ansible targets -m file -a "dest=/home/ubuntu/mountdfc.sh mode=777 owner=ubuntu group=ubuntu" -i inventory/cluster.ini --become
ansible targets -m shell -a "/home/ubuntu/mountdfc.sh > mountdfc.log" -i inventory/cluster.ini --become
ansible targets -m shell -a "mount | grep dfc" -i inventory/cluster.ini --become
parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/startproxy.sh >/dev/null 2>&1'
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/starttarget.sh >/dev/null 2>&1'
sleep 10
parallel-ssh -h inventory/proxy.txt -i 'cat ~/dfc.json'
parallel-ssh -h inventory/proxy.txt -i 'tail -20 /var/log/dfc/dfc.INFO'
