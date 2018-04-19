#!/bin/bash
parallel-ssh -h inventory/targets.txt -i 'ps -C dfc -o pid= | xargs sudo kill || true'
parallel-ssh -h inventory/targets.txt -i 'ps -C dfc -o pid= | xargs sudo kill || true'
parallel-ssh -h inventory/proxy.txt -i 'ps -C dfc -o pid= | xargs sudo kill'
parallel-ssh -h inventory/proxy.txt -i 'sudo rm -rf /var/log/dfc*'
parallel-ssh -h inventory/targets.txt -i 'sudo rm -rf /var/log/dfc*'
