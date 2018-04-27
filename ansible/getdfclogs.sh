#!/bin/bash
set -x
mkdir logs
parallel-ssh -h inventory/proxy.txt -i 'sudo tar czf /tmp/primaryproxy_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/dfcproxy >/dev/null 2>&1'
parallel-ssh -h inventory/targets.txt -i 'sudo tar czf /tmp/stubproxy_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/dfcproxy >/dev/null 2>&1'
parallel-ssh -h inventory/targets.txt -i 'sudo tar czf /tmp/target_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/dfc >/dev/null 2>&1'
parallel-ssh -h inventory/cluster.txt -i 'ls /tmp'
for ip in `cat inventory/proxy.txt`; do echo $ip; scp $ip:/tmp/*.tar.gz logs/; ssh $ip sudo rm -rf /tmp/*_$(hostname)_*.tar.gz; done
for ip in `cat inventory/targets.txt`; do echo $ip; scp $ip:/tmp/*.tar.gz logs/; ssh $ip sudo rm -rf /tmp/*_$(hostname)_*.tar.gz; done
ls -al logs
for ip in `cat inventory/proxy.txt`; do ssh $ip 'sudo rm -rf /tmp/*_$(hostname)_*.tar.gz'; done
for ip in `cat inventory/targets.txt`; do ssh $ip 'sudo rm -rf /tmp/*_$(hostname)_*.tar.gz'; done
parallel-ssh -h inventory/cluster.txt -i 'ls /tmp'

