#!/bin/bash
parallel-ssh -h inventory/targets.txt -P 'for dfcpid in `ps -C dfc -o pid=`; do echo Stopping DFC $dfcpid; sudo kill $dfcpid; done'
parallel-ssh -h inventory/new_targets.txt -P 'for dfcpid in `ps -C dfc -o pid=`; do echo Stopping DFC $dfcpid; sudo kill $dfcpid; done'
parallel-ssh -h inventory/proxy.txt -i 'ps -C dfc -o pid= | xargs sudo kill'
parallel-ssh -h inventory/proxy.txt -i 'sudo rm -rf /var/log/dfc*'
parallel-ssh -h inventory/targets.txt -i 'sudo rm -rf /var/log/dfc*'
