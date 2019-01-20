#!/bin/bash
parallel-ssh -h inventory/targets.txt -P 'for aispid in `ps -C ais -o pid=`; do echo Stopping AIS $aispid; sudo kill $aispid; done'
parallel-ssh -h inventory/new_targets.txt -P 'for aispid in `ps -C ais -o pid=`; do echo Stopping AIS $aispid; sudo kill $aispid; done'
parallel-ssh -h inventory/proxy.txt -i 'ps -C ais -o pid= | xargs sudo kill'
parallel-ssh -h inventory/proxy.txt -i 'sudo rm -rf /var/log/ais*'
parallel-ssh -h inventory/targets.txt -i 'sudo rm -rf /var/log/ais*'
