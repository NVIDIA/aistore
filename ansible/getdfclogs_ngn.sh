#!/bin/bash
set -x
LOGDIR=logs_$(date +%Y%m%d-%H%M%S)
echo collecting logs in $LOGDIR
mkdir -p $LOGDIR
parallel-ssh -h inventory/proxy.txt -i 'sudo tar czf /tmp/primaryproxy_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/ais_proxy /etc/ais/bucket-metadata /etc/ais/smap.json  >/dev/null 2>&1'
parallel-ssh -h inventory/targets.txt -i 'sudo tar czf /tmp/target_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /var/log/ais_target >/dev/null 2>&1'
parallel-ssh -h inventory/clients.txt -i 'sudo tar czf /tmp/client_$(hostname)_$(date +%Y%m%d-%H%M%S).tar.gz /home/ubuntu/ais/src/github.com/NVIDIA/dfcpub/bench/aisloader/screenlog.0 >/dev/null 2>&1'
for ip in `cat inventory/proxy.txt`; do echo $ip; scp $ip:/tmp/*.tar.gz $LOGDIR/; ssh $ip 'sudo rm -rf /tmp/*_$(hostname)_*.tar.gz'; done
for ip in `cat inventory/targets.txt`; do echo $ip; scp $ip:/tmp/*.tar.gz $LOGDIR/; ssh $ip 'sudo rm -rf /tmp/*_$(hostname)_*.tar.gz'; done
for ip in `cat inventory/clients.txt`; do echo $ip; scp $ip:/tmp/*.tar.gz $LOGDIR/; ssh $ip 'sudo rm -rf /tmp/*_$(hostname)_*.tar.gz'; done
ls -al $LOGDIR
