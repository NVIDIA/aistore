#!/bin/bash
set -e
parallel-ssh -h inventory/cluster.txt -i 'ls /tmp'
parallel-ssh -h inventory/targets.txt -i 'sudo tar czf /tmp/stubproxy_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/dfcproxy >/dev/null 2>&1'
parallel-ssh -h inventory/targets.txt -i 'sudo tar czf /tmp/target_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/dfc >/dev/null 2>&1'
parallel-ssh -h inventory/cluster.txt -i 'ls /tmp'
for ip in `cat inventory/cluster.txt`; do echo $ip; scp $ip:/tmp/*.tar.gz logs/; done
ls -al logs
parallel-ssh -h inventory/proxy.txt -i 'sudo tar czf /tmp/primaryproxy_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/dfc >/dev/null 2>&1'
parallel-ssh -h inventory/cluster.txt -i 'sudo rm -rf /tmp/*_$(hostname)_*.tar.gz'
parallel-ssh -h inventory/cluster.txt -i 'ls /tmp'

