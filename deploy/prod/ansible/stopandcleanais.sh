#!/bin/bash
parallel-ssh -h inventory/targets.txt -P 'for aispid in `ps -C aisnode -o pid=`; do echo Stopping AIS $aispid; sudo kill $aispid; done'
if [[ -s inventory/new_targets.txt ]]; then parallel-ssh -h inventory/new_targets.txt -P 'for aispid in `ps -C aisnode -o pid=`; do echo Stopping AIS $aispid; sudo kill $aispid; done'; fi
parallel-ssh -h inventory/proxy.txt -i 'ps -C aisnode -o pid= | xargs sudo kill'
parallel-ssh -h inventory/proxy.txt -i 'sudo rm -rf /var/log/ais*'
parallel-ssh -h inventory/targets.txt -i 'sudo rm -rf /var/log/ais*'
