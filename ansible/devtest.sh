#!/bin/bash
cd $1/dfcpub/ansible
parallel-scp -h inventory/targets.txt rundevtest.sh '/home/ubuntu/'
ssh $(head -1 inventory/targets.txt) './rundevtest.sh master'
EXIT_STATUS=$?
echo RUNTEST exit status is $EXIT_STATUS
ssh $(head -1 inventory/targets.txt) 'sudo tar czf /tmp/devtest_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /tmp/dfc /home/ubuntu/.dfc* >/dev/null 2>&1'
mkdir logs
scp $(head -1 inventory/targets.txt):/tmp/*.tar.gz logs/
echo DevTest logs are copied here 
pwd
ls -al logs

exit $EXIT_STATUS
