#!/bin/bash
set -e

echo Starting primary proxy
parallel-ssh -h inventory/proxy.txt -i 'nohup /home/ubuntu/startprimaryproxy.sh >/dev/null 2>&1'
sleep 5
echo Starting stub proxies on every target node
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/startproxy.sh >/dev/null 2>&1'
sleep 5
echo Starting all targets
parallel-ssh -h inventory/targets.txt -i 'nohup /home/ubuntu/starttarget.sh >/dev/null 2>&1'
echo Wait 20 seconds before checking AIS health
sleep 20
parallel-ssh -h inventory/proxy.txt -i 'ps -C ais'
parallel-ssh -h inventory/targets.txt -i 'ps -C ais'
parallel-ssh -h inventory/proxy.txt -i 'cat ~/ais.json'
parallel-ssh -h inventory/proxy.txt -i 'tail -20 /var/log/aisproxy/ais.INFO'
parallel-ssh -h inventory/targets.txt -i 'tail -20 /var/log/ais/ais.INFO'
parallel-ssh -h inventory/targets.txt -i 'tail -20 /var/log/aisproxy/ais.INFO'


