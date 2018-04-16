#!/bin/bash
set -o xtrace
set -e

parallel-ssh -h inventory/clients.txt -i './startclient.sh 100 5m'

clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l`
sleep 30
while [ $clients_running -gt 0 ]; do 
	clients_running=`parallel-ssh -h inventory/clients.txt -i "screen -ls client" | grep client | wc -l` 
	echo running 
	parallel-ssh -h inventory/clients.txt -i 'tail -10 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/cmd/dfcloader/screenlog.0'
	parallel-ssh -h inventory/targets.txt -i "iostat -xm 5 -c 2 | tail -33"
	parallel-ssh -h inventory/targets.txt -i "netstat -s | grep transmit"
	sleep 10 
done

parallel-ssh -h inventory/clients.txt -i 'tail -10 /home/ubuntu/dfc/src/github.com/NVIDIA/dfcpub/cmd/dfcloader/screenlog.0'
