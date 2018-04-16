parallel-ssh -h inventory/targets.txt -i 'ps -C dfc -o pid= | xargs sudo kill -9'
parallel-ssh -h inventory/proxy.txt -i 'ps -C dfc -o pid= | xargs sudo kill -9'
parallel-ssh -h inventory/proxy.txt -i 'sudo rm -rf /var/log/dfc*'
parallel-ssh -h inventory/targets.txt -i 'sudo rm -rf /var/log/dfc*'
